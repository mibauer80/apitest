const express = require('express');
const bodyParser = require("body-parser");
const process = require('process');
const mysql = require('mysql');
const {
    callbackify
} = require('util');

const app = express();

app.use(function (req, res, next) {
    res.header("Access-Control-Allow-Origin", "*"); // update to match the domain you will make the request from
    res.header("Access-Control-Allow-Headers", "*");
    res.header("Access-Control-Allow-Methods", "POST, GET, METHODS");
    next();
});

app.use(bodyParser.urlencoded({
    extended: true
}));
app.use(bodyParser.json());

var config = {
    user: 'root',
    database: 'shoppinglist',
    password: 'schoemBerg1994',
    timezone: 'utc'
}

if (process.env.INSTANCE_CONNECTION_NAME && process.env.NODE_ENV === 'production') {
    config.socketPath = `/cloudsql/${process.env.INSTANCE_CONNECTION_NAME}`;
}

var connection = mysql.createConnection(config);

connection.connect();

function updateListItem(listItemId, listId, quantity, posId, urgent, saleStart, saleEnd, callback) {
    checkItemData(quantity, urgent, posId, saleStart, saleEnd, function (success, response) {
        if (!success) {
            return callback(false, response);
        } else {
            var sql1 = "UPDATE list_items SET quantity = ?, urgent = ?, sale_start = ?, sale_end = ? WHERE id = ? AND list_id = ?";
            var values = [quantity, urgent, saleStart, saleEnd, listItemId, listId];
            connection.query(sql1, values, function (err, result1) {
                if (err) {
var response = {'code': 901, 'messages': [err.code, err.sqlMessage, err.sql], 'options': [{'text': 'Abbrechen', 'action': 'deleteModal', 'payload':[]}]};
callback(false, response);
}
                if (result1.changedRows != 1) {
                    var response = {
                        code: 302,
                        messages: ['Keine geänderten Werte'],
                        listItemIds: [listItemId],
                        options: []
                    };
                    callback(false, response);
                } else {
                    var response = {
                        code: 104,
                        messages: ['Eintrag wurde aktualisiert'],
                        listItemIds: [listItemId],
                        options: []
                    };
                    callback(true, response);
                }
            });
        }
    });
}

function checkItemData(quantity, urgent, posId, saleStart, saleEnd, callback) {
    var errors = [];
    var response = {};
    var dateRegex = /^(\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01]))?$/;
    if (typeof quantity === 'undefined' || quantity < 1 || quantity > 20) errors.push('Ungültige Mengenangabe: ' + quantity);
    if (!/^[0,1,2]?$/.test(urgent)) errors.push('Falsches Format für Dringlichkeitskennzeichnung: ' + urgent);
    if (!dateRegex.test(saleStart || '')) errors.push('Falsches Datumsformat für Angebotsbeginn: ' + saleStart);
    if (!dateRegex.test(saleEnd || '')) errors.push('Falsches Datumsformat für Angebotsende: ' + saleEnd);
    if (!saleStart && saleEnd) errors.push('Fehlende Angabe zum Angebotsbeginn!');
    if (saleStart && !saleEnd) errors.push('Fehlende Angabe zum Angebotsende!');
    if (saleStart && saleEnd) {
        var s_start = saleStart.split('-');
        var d_start = new Date(s_start[0], s_start[1], s_start[2]);
        var s_end = saleEnd.split('-');
        var d_end = new Date(s_end[0], s_end[1], s_end[2]);
        if (d_start > d_end) errors.push('Angebotsbeginn nach Angebotsende!');
    }
    if (typeof posId === 'undefined' && (typeof saleStart !== 'undefined' || typeof saleEnd !== 'undefined'))
        errors.push('Angebotszeitraum ohne Angabe einer Verkaufstelle!');   
    if (errors.length > 0) {
        response.code = 902;
        response.messages = errors;
        response.listItemIds = [];
        response.options = [];
        return callback(false, response);
    } else {
        return callback(true, 'valid item data')
    }
};

function insertListItem(listId, productId, catId, quantity, urgent, posId, saleStart, saleEnd, force, callback) {
	console.log(JSON.stringify(arguments));
    checkItemData(quantity, urgent || 0, posId, saleStart, saleEnd, function (success, response) {
        if (!success) {
            return callback(false, response);
        } else {
        	var response = {};
            var sql1 = "SELECT * FROM list_items WHERE list_id = ? AND product_id = ?";
            var values = [listId, productId]
            var query = connection.query(sql1, values, function (err, result1, fields) {
            	console.log(query.sql);
                if (err) {
response = {'code': 901, 'messages': [err.code, err.sqlMessage, err.sql], 'options': [{'text': 'Abbrechen', 'action': 'deleteModal', 'payload':[]}]};
return callback(false, response);}
                
                var posId_null = Number(posId) || null;
                var matching = result1.find(i => i.pos_id === posId_null);
                console.log(JSON.stringify(result1));
                console.log(JSON.stringify(matching));
                if (typeof matching !== 'undefined') {
                    var changeNotes = '';
                    var item_mod = [];
                    if (matching.quantity != quantity) {
                        var text = matching.quantity > quantity ? 'reduzieren' : 'erhöhen';
                        item_mod.push('Menge ' + text);
                    }
                    if (matching.urgent != urgent) {
                        var urgency = ['optional', 'normal', 'Notstand'];
                        item_mod.push('Dringlichkeit ändern: ' + urgency[Number(urgent)]);
                    }
                    if (Number(matching.sale_start) === 0 && typeof saleStart !== 'undefined') item_mod.push('Angebotsbeginn hinzufügen');
                    if (Number(matching.sale_end) === 0 && typeof saleEnd !== 'undefined') item_mod.push('Angebotsende hinzufügen');
                    var startIso = !matching.sale_start ? 0 : matching.sale_start.toISOString().slice(0, 10);
                    var endIso = !matching.sale_end ? 0 : matching.sale_end.toISOString().slice(0, 10);
                    if (startIso != saleStart && typeof saleStart !== 'undefined') item_mod.push('Angebotsbeginn ändern');
                    if (endIso != saleEnd && typeof saleEnd !== 'undefined') item_mod.push('Angebotsende ändern');
                    if (Number(matching.sale_start) !== 0 && typeof saleStart === 'undefined') item_mod.push('Angebotsbeginn entfernen');
                    if (Number(matching.sale_end) !== 0 && typeof saleEnd === 'undefined') item_mod.push('Angebotsende entfernen');
                    options = [];
                    if (item_mod.length > 0) {
                        options.push({
                            text: 'Eintrag aktualisieren',
                            action: 'updateItem',
                            payload: [matching.id, 1, quantity, posId, urgent, saleStart, saleEnd]
                        });
                        options.push({
                            text: 'Abbrechen',
                            action: 'deleteModal',
                            payload: []
                        });
                        changeNotes =  ' mit abweichenden Angaben:<br>' + item_mod.join('</br>');
                    }
                    response.code = 201;
                    response.messages = ['Eintrag für Produkt bei dieser Verkaufsstelle existiert bereits' + changeNotes];
                    response.listItemIds = [matching.id];
                    response.options = options;
                    return callback(true, response);
                }   else if (force !== true && typeof posId !== 'undefined' && typeof (lid = result1.find(i => !i.pos_id)) !== 'undefined') {
                    response.code = 202;
                    response.messages = ['Produkt bereits ohne konkrete Verkaufsstelle notiert!'];
                    response.listItemIds = [lid];
                    response.options = [{
                            text: 'Unspezifischen Eintrag löschen',
                            action: 'deleteItem',
                            payload: [lid]
                        },                       
                        {
                            text: 'Beide Einträge behalten',
                            action: 'addItem',
                            payload: [productId, catId, quantity, posId, urgent, saleStart, saleEnd, true]
                        }
                    ];
                    return callback(true, response);
                } else if (force !== true && typeof posId === 'undefined' && Object.keys(result1.filter(i => i.pos_id > 0)).length !== 0) {
                	console.log('force:' + force + ', typeof posId: ' + typeof posId  + ', Object.keys(result1.filter(i => i.pos_id > 0)).length: ' + Object.keys(result1.filter(i => i.pos_id > 0)).length + ' | ' + JSON.stringify(result1.filter(i => i.pos_id > 0)));
                    var lids = (lids = result1.filter(i => i.pos_id > 0));
                    response.code = 203;
                    response.messages = ['Produkt bereits mit konkreter Verkaufsstelle notiert!'];
                    response.listItemIds = lids;
                    response.options = [{
                            text: 'Abbrechen',
                            action: 'deleteModal',
                            payload: lids
                        },
                        {
                            text: 'Unspezifischen Eintrag hinzufügen',
                            action: 'addItem',
                            payload: [productId, catId, quantity, posId, urgent, saleStart, saleEnd, true]
                        }
                    ];
                    return callback(true, response);
                } else {
                    var sql2 = "INSERT INTO list_items (list_id, product_id, quantity, urgent, pos_id, sale_start, sale_end) values (?, ?, ?, ?, ?, ?, ?)";
                    var values = [listId, productId, quantity, urgent, posId, saleStart, saleEnd];
                    connection.query(sql2, values, function (err, result2) {
                        if (err) {
                        	response = {'code': 901, 'messages': [err.code, err.sqlMessage, err.sql], 'options': [{'text': 'Abbrechen', 'action': 'deleteModal', 'payload':[]}]};
                        	return callback(false, response);
                        }
                        response.code = 101;
                        response.messages = ['Produkt wurde in Merkliste eingetragen'];
                        response.listItemIds = result2.insertId;
                        response.options = [];
                        return callback(true, response);
                    });
                }
            });
        }
    });
}

function insertProduct(name, catId, callback) {
    var sql = "INSERT INTO products (name, cat_id) values (?, ?)";
    var values = [name, catId];
    connection.query(sql, values, function (err, result) {
        if (err) {
        	var response = {'code': 901, 'messages': [err.code, err.sqlMessage, err.sql], 'options': [{'text': 'Abbrechen', 'action': 'deleteModal', 'payload':[]}]};
            return callback(false, response);
        }
        return callback(true, result)
    });
}

app.get('/items/list/:id', (req, res) => {
    var sql = "SELECT i.id AS item_id, i.product_id, p.name AS product_name, p.cat_id AS catId, i.quantity, s.id AS pos_id, s.name AS pos_name, i.urgent, i.sale_start, i.sale_end FROM list_items i LEFT JOIN products p ON i.product_id = p.id LEFT JOIN pos s ON i.pos_id = s.id WHERE list_id = ?";
    var values = req.params.id;
    connection.query(sql, values, function (err, result) {
        if (err) {
var response = {'code': 901, 'messages': [err.code, err.sqlMessage, err.sql], 'options': [{'text': 'Abbrechen', 'action': 'deleteModal', 'payload':[]}]};
        	return res.status(200).send(response);
        }
        if (result.length === 0) {
            var response = {
                code: 301,
                messages: ['Liste enthält keine Einträge'],
                options: []
            };
            res.status(200).send(response);
        } else {
            return res.status(200).send(result);
        }
    });
});

app.get('/pos/list', (req, res) => {
    var sql = "SELECT * FROM pos ORDER BY name ASC";
    var values;
    connection.query(sql, values, function (err, result) {
        if (err) {
        	var response = {'code': 901, 'messages': [err.code, err.sqlMessage, err.sql], 'options': [{'text': 'Abbrechen', 'action': 'deleteModal', 'payload':[]}]};
        	return res.status(200).send(response);
        }
        if (result.length === 0) {
            var response = {
                code: 301,
                messages: ['Keine Einträge'],
                options: []
            };
            res.status(200).send(response);
        } else {
            return res.status(200).send(result);
        }
    });
});

app.get('/products/list', (req, res) => {
    var sql = "SELECT * FROM products ORDER BY name ASC";
    var values;
    connection.query(sql, values, function (err, result) {
        if (err) {
        	var response = {'code': 901, 'messages': [err.code, err.sqlMessage, err.sql], 'options': [{'text': 'Abbrechen', 'action': 'deleteModal', 'payload':[]}]};
        	return res.status(200).send(response);
        }
        if (result.length === 0) {
            var response = {
                code: 301,
                messages: ['Keine Einträge'],
                options: []
            };
            res.status(200).send(response);
        } else {
            return res.status(200).send(result);
        }
    });
});

app.get('/categories/list', (req, res) => {
    var sql = "SELECT * FROM categories ORDER BY title ASC";
    var values;
    connection.query(sql, values, function (err, result) {
        if (err) {
        	var response = {'code': 901, 'messages': [err.code, err.sqlMessage, err.sql], 'options': [{'text': 'Abbrechen', 'action': 'deleteModal', 'payload':[]}]};
        	return res.status(200).send(response);
        }
        if (result.length === 0) {
            var response = {
                code: 301,
                messages: ['Keine Einträge'],
                options: []
            };
            res.status(200).send(response);
        } else {
            return res.status(200).send(result);
        }
    });
});

app.post('/items/insert', (req, res) => {
    var values = req.body.name;
    if (typeof req.body.name === 'number') {
        var sql = "SELECT * FROM products WHERE id = ?";
    
    connection.query(sql, values,
        function (err, result1) {
            if (err) {
            	var response = {'code': 901, 'messages': [err.code, err.sqlMessage, err.sql], 'options': [{'text': 'Abbrechen', 'action': 'deleteModal', 'payload':[]}]};
                return res.status(200).send(response);
            }
            if (result1.length === 0) {
                var response = {
                    code: 302,
                    messages: ['Angegebene Produkt-ID existiert nicht.'],
                    options: []
                };
                return res.status(200).send(response);
            } else {
                insertListItem(1, result1[0].id, result1[0].cat_id, req.body.quantity, req.body.urgent || 0, req.body.posId, req.body.saleStart, req.body.saleEnd, req.body.force, function (success, response) {
                    if (!success) {
                        return res.status(200).send(response);
                    }
                    return res.status(200).send(response);
                });
            }        
        });
    } else {
    var sql = "SELECT * FROM products WHERE name = ?";    
    connection.query(sql, values,
        function (err, result1) {
            if (err) {
            	var response = {'code': 901, 'messages': [err.code, err.sqlMessage, err.sql], 'options': [{'text': 'Abbrechen', 'action': 'deleteModal', 'payload':[]}]};
                return res.status(200).send(response);
            }
            if (result1.length === 0) {
                insertProduct(req.body.name, req.body.catId, function (success, response) {
                    if (!success) {
                        return res.status(200).send(response);
                    }
                    insertListItem(1, response.insertId, req.body.catId, req.body.quantity, req.body.urgent || 0, req.body.posId, req.body.saleStart, req.body.saleEnd, req.body.force, function (success, response) {
                        if (!success) {
                            return res.status(200).send(response);
                        }
                        return res.status(200).send(response);
                    });
                });
            } else {
                insertListItem(1, result1[0].id, result1[0].cat_id, req.body.quantity, req.body.urgent || 0, req.body.posId, req.body.saleStart, req.body.saleEnd, req.body.force , function (success, response) {
                    if (!success) {
                        return res.status(200).send(response);
                    }
                    return res.status(200).send(response);
                });
            }
        });
    }
});

app.post('/items/update', (req, res) => {
    updateListItem(req.body.listItemId, 1, req.body.quantity, req.body.posId, req.body.urgent || 0, req.body.saleStart, req.body.saleEnd, function (success, response) {
        if (!success) {
            return res.status(200).send(response);
        }
        return res.status(200).send(response);
    });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`App listening on port ${PORT}`);
    console.log('Press Ctrl+C to quit.');
});