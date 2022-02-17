const express = require('express');
const bodyParser = require("body-parser");
const process = require('process');
const {
    callbackify
} = require('util');

const {
    BigQuery
} = require('@google-cloud/bigquery');
const options = {
    keyFilename: 'mykey.json',
    projectId: 'proven-aviary-293214',
};
const bigquery = new BigQuery(options);

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

function updateListItem(listItemId, listId, quantity, posId, urgent, saleStart, saleEnd, callback) {
    checkItemData(quantity, urgent, posId, saleStart, saleEnd, function (success, response) {
        if (!success) {
            return callback(false, response);
        } else {
            const bq_options = {
                query: 'UPDATE list_items SET quantity = ' + quantity + ', urgent = ' + urgent + ', sale_start = ' + sale_start + ', sale_end = ' + sale_end + ' WHERE id = ' + listItemId + ' AND list_id = ' + listId,
                location: 'EU',
            };
            bigquery.query(bq_options, (err, rows1) => {
                if (err) {
                    var response = {
                        'code': 901,
                        'type': 'error',
                        'messages': [err.code, err.sqlMessage, err.sql]
                    };
                    callback(false, response);
                }
                if (rows1.changedrows1 != 1) {
                    var response = {
                        code: 302,
                        type: 'error',
                        messages: ['Keine geänderten Werte'],
                        listItemIds: [listItemId],
                        options: []
                    };
                    callback(false, response);
                } else {
                    var response = {
                        code: 104,
                        type: 'success',
                        title: 'Aktualisierung',
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
    console.log('typeof posId: ' + typeof posId + '(' + posId + ')');
    console.log('typeof saleStart: ' + typeof saleStart + '(' + JSON.stringify(saleStart) + ')');
    console.log('posId === 0 && typeof saleStart !== undefined: ' + posId === 0 && typeof saleStart !== 'undefined');
    var errors = [];
    var response = {};
    var dateRegex = /^(\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01]))?$/;
    if (quantity < 1 || quantity > 20) errors.push('Ungültige Mengenangabe: ' + quantity);
    if (quantity < 1 || quantity > 20) errors.push('Ungültige Mengenangabe: ' + quantity);
    if (typeof posId === 'undefined' || posId < 0 || posId > 4) errors.push('Ungültige Angabe zur Verkaufsstelle: ' + posId);
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
    if (posId === 0 && (typeof saleStart !== 'undefined' || typeof saleEnd !== 'undefined'))
        errors.push('Angebotszeitraum ohne Angabe einer Verkaufstelle!');
    if (errors.length > 0) {
        response.code = 902;
        response.type = 'error';
        response.title = 'Fehlerhafte Angaben';
        response.messages = errors;
        response.listItemIds = [];
        response.options = [];
        return callback(false, response);
    } else {
        return callback(true, 'valid item data')
    }
};

function insertListItem(listId, productId, catId, quantity, urgent, posId, saleStart, saleEnd, force, callback) {
    var posId = Number(posId);
    checkItemData(quantity, urgent || 0, posId, saleStart, saleEnd, function (success, response) {
        if (!success) {
            return callback(false, response);
        } else {
            var response = {};
            const bq_options = {
                query: 'SELECT * FROM shoppinglist.list_items WHERE list_id = ' + listId + ' AND product_id = ' + productId + ';',
                location: 'EU',
            };

            bigquery.query(bq_options, (err, rows1) => {
                console.log(bq_options.query);
                if (err) {
                    response = {
                        'code': 901,
                        'type': 'error',
                        'messages': JSON.stringify(err),
                        'options': []
                    };
                    return callback(false, response);
                }

                var matching = rows1.find(i => i.pos_id === posId);
                console.log('insertListItem(' + posId + ')');
                if (typeof matching !== 'undefined') {
                    var changeNotes = '';
                    var item_mod = [];
                    if (matching.quantity != quantity) {
                        var text = matching.quantity > quantity ? 'reduziert' : 'erhöht';
                        item_mod.push('Menge ' + text + ':<br>' + matching.quantity + ' &rarr; ' + quantity);
                    }
                    if (matching.urgent != urgent) {
                        var urgency = ['optional', 'normal', 'Notstand'];
                        item_mod.push('Dringlichkeit geändert:<br>' + urgency[Number(matching.urgent)] + ' &rarr; ' + urgency[Number(urgent)]);
                    }
                    if (Number(matching.sale_start) === 0 && typeof saleStart !== 'undefined') item_mod.push('Angebotsbeginn hinzugefügt');
                    if (Number(matching.sale_end) === 0 && typeof saleEnd !== 'undefined') item_mod.push('Angebotsende hinzugefügt');
                    var startIso = !matching.sale_start ? 0 : matching.sale_start.toISOString().slice(0, 10);
                    var endIso = !matching.sale_end ? 0 : matching.sale_end.toISOString().slice(0, 10);
                    if (startIso != saleStart && typeof saleStart !== 'undefined') item_mod.push('Angebotsbeginn geändert:<br>' + startIso + ' &rarr; ' + saleStart);
                    if (endIso != saleEnd && typeof saleEnd !== 'undefined') item_mod.push('Angebotsende geändert:<br>' + endIso + ' &rarr; ' + saleEnd);
                    if (Number(matching.sale_start) !== 0 && typeof saleStart === 'undefined') item_mod.push('Angebotsbeginn entfernt');
                    if (Number(matching.sale_end) !== 0 && typeof saleEnd === 'undefined') item_mod.push('Angebotsende entfernt');
                    if (item_mod.length > 0) {
                        options.push({
                            text: 'Eintrag aktualisieren',
                            action: 'updateItem',
                            type: 'function',
                            payload: [matching.id, 1, quantity, posId, urgent, saleStart, saleEnd]
                        });
                        changeNotes = ', jedoch mit abweichenden Angaben:<br><br><ul><li>' + item_mod.join('</li><li>') + '</li></ul>';
                    }
                    response.code = 201;
                    response.type = item_mod.length > 0 ? 'confirm' : 'error';
                    response.title = 'Produkt bereits eingetragen';
                    response.messages = ['Produkt ist bei dieser Verkaufsstelle bereits notiert' + changeNotes];
                    response.listItemIds = [matching.id];
                    response.options = options;
                    return callback(true, response);
                } else if (force !== true && posId !== 0 && typeof rows1.find(i => i.pos_id === 0) !== 'undefined') {
                    var lid = rows1.find(i => i.pos_id === 0).id;
                    response.code = 202;
                    response.type = 'confirm';
                    response.title = 'Produkt bereits eingetragen';
                    response.messages = ['Produkt bereits ohne konkrete Verkaufsstelle notiert! Ein Produkt kann nicht gleichzeitig unter "Allgemein" und für eine bestimmte Verkaufsstelle eingetragen werden.'];
                    response.listItemIds = [lid];
                    response.options = [{
                        text: 'Alten Eintrag löschen',
                        action: 'deleteItem',
                        type: 'function',
                        payload: [lid, listId, productId, catId, quantity, posId, urgent, saleStart, saleEnd, force]
                    }];
                    return callback(true, response);
                } else if (force !== true && posId === 0 && typeof rows1.find(i => i.pos_id !== 0) !== 'undefined') {
                    var lid = rows1.find(i => i.pos_id !== 0).id;
                    response.code = 203;
                    response.type = 'confirm';
                    response.title = 'Produkt bereits eingetragen';
                    response.messages = ['Produkt bereits mit konkreter Verkaufsstelle notiert! Ein Produkt kann nicht gleichzeitig unter "Allgemein" und für eine bestimmte Verkaufsstelle eingetragen werden.'];
                    response.listItemIds = [lid];
                    response.options = [{
                        text: 'Alten Eintrag löschen',
                        action: 'deleteItem',
                        type: 'function',
                        payload: [lid, listId, productId, catId, quantity, posId, urgent, saleStart, saleEnd, force]
                    }];
                    return callback(true, response);
                } else {

                    const bq_options = {
                        query: 'DELETE FROM list_items WHERE (sale_end IS NOT NULL AND sale_end < curdate());',
                        location: 'EU',
                    };

                    bigquery.query(bq_options);

                    const dataset = bigquery.dataset('shoppinglist');
                    const table = dataset.table('list_items');
                    const insertRow = {
                        list_id: listId,
                        product_id: productId,
                        quantity: quantity,
                        urgent: urgent,
                        pos_id: posId,
                        sale_start: saleStart,
                        sale_end: saleEnd
                    }
                    table.insert(insertRow, function (err, result2) {
                        console.log(JSON.stringify(insertRow))
                        if (err) {
                            response = {
                                'code': 901,
                                'type': 'error',
                                'messages': JSON.stringify(err),
                                'options': []
                            };
                            return callback(false, response);
                        }
                        response.code = 101;
                        response.type = 'success';
                        response.title = 'Eintrag erfolgt';
                        response.messages = ['Produkt wurde auf dem Einkaufszettel notiert'];
                        response.listItemIds = result2.insertId;
                        response.options = [];
                        return callback(true, response);
                    });
                }
            });
        }
    });
}

function itemsCart(listItemId, listId, cartStatus, callback) {

    const bq_options = {
        query: 'UPDATE list_items SET cart_status = ' + cartStatus + ' WHERE id = ' + listItemId + ' AND list_id = ' + listId + ';',
        location: 'EU',
    };

    bigquery.query(bq_options, (err, result) => {
        if (err) {
            var response = {
                'code': 901,
                'type': 'error',
                'messages': [err.code, err.sqlMessage, err.sql],
                'options': []
            };
            return callback(false, response);
        }
        if (result.changedrows1 != 1) {
            var response = {
                code: 303,
                type: 'error',
                messages: ['Keine geänderten Werte'],
                listItemIds: [listItemId],
                options: []
            };
            callback(false, response);
        } else {
            var text = cartStatus === 1 ? 'in den Warenkorb gelegt' : 'aus dem Warenkorb entfernt';
            var response = {
                code: 105,
                type: 'success',
                title: 'Warenkorb aktualisiert',
                messages: ['Produkt wurde ' + text],
                listItemIds: [listItemId],
                options: []
            };
            return callback(true, response);
        }
    });
}

function itemsDelete(listItemId, listId, callback) {
    const bq_options = {
        query: 'DELETE FROM list_items WHERE id = ' + listItemId + ' AND list_id = ' + listId + ';',
        location: 'EU',
    };
    bigquery.query(bq_options, (err, result) => {
        if (err) {
            var response = {
                'code': 901,
                'type': 'error',
                'messages': [err.code, err.sqlMessage, err.sql],
                'options': []
            };
            return callback(false, response);
        }
        if (result.affectedrows1 != 1) {
            var response = {
                code: 303,
                type: 'error',
                messages: ['Keine geänderten Werte'],
                listItemIds: [listItemId],
                options: []
            };
            callback(false, response);
        } else {

            var response = {
                code: 105,
                type: 'success',
                title: 'Eintrag entfernt',
                messages: ['Produkt wurde vom Einkaufszettel gelöscht'],
                listItemIds: [listItemId],
                options: []
            };
            return callback(true, response);
        }
    });
}

function itemsCheckout(listId, posId, callback) {
    const bq_options = {
        query: 'DELETE FROM list_items WHERE cart_status = 1 AND list_id = ' + listId + ' AND (pos_id = ' + posId + ' OR pos_id = 0);',
        location: 'EU',
    };
    bigquery.query(bq_options, (err, result) => {
        if (err) {
            var response = {
                'code': 901,
                'type': 'error',
                'messages': [err.code, err.sqlMessage, err.sql],
                'options': []
            };
            return callback(false, response);
        }
        if (result.affectedrows1 < 1) {
            var response = {
                code: 306,
                type: 'error',
                messages: ['Keine geänderten Werte'],
                posIds: [posId],
                options: []
            };
            callback(false, response);
        } else {

            var response = {
                code: 106,
                type: 'success',
                title: 'Checkout abgeschlossen',
                messages: ['Produkte im Warenkorb wurden aus der Liste entfernt'],
                posIds: [posId],
                options: []
            };
            return callback(true, response);
        }
    });
}

function insertProduct(name, catId, callback) {
    var response = {};
    const dataset = bigquery.dataset('shoppinglist');
    const table = dataset.table('products');
    const rowId = new Date().getTime();
    const insertRow = {
        id: rowId,
        name: name,
        cat_id: catId
    }
    table.insert(insertRow, function (err, result) {
        console.log(JSON.stringify(insertRow))
        if (err) {
            response = {
                'code': 901,
                'type': 'error',
                'messages': JSON.stringify(err),
                'options': []
            };
            return callback(false, response);
        }
        response.code = 101;
        response.type = 'success';
        response.title = 'Eintrag erfolgt';
        response.messages = ['Produkt wurde auf dem Einkaufszettel notiert'];
        response.productId = rowId;
        response.options = [];
        return callback(true, response);
    });
}

app.get('/items/list/:id', (req, res) => {
    const bq_options = {
        query: 'SELECT i.id AS item_id, i.product_id, p.name AS product_name, p.cat_id AS catId, i.quantity, i.cart_status AS cart_status, s.id AS pos_id, s.name AS pos_name, i.urgent, i.sale_start, i.sale_end FROM shoppinglist.list_items i LEFT JOIN shoppinglist.products p ON i.product_id = p.id LEFT JOIN shoppinglist.pos s ON i.pos_id = s.id WHERE list_id = ' + req.params.id + ' AND (sale_end IS NULL OR sale_end >= CURRENT_DATE())',
        location: 'EU',
    };
    bigquery.query(bq_options, (err, rows1) => {
        console.log(bq_options.query);
        if (err) {
            response = {
                'code': 901,
                'type': 'error',
                'messages': JSON.stringify(err),
                'options': []
            };
            return res.status(200).send(response);
        }
        return res.status(200).send(rows1);
    });
});

app.get('/pos/list', (req, res) => {

    const options = {
        query: 'SELECT * FROM shoppinglist.pos ORDER BY name ASC',
        location: 'EU',
    };

    bigquery.query(options, (err, rows1) => {
        if (err) {
            var response = {
                'code': 901,
                'type': 'error',
                'messages': [err.code, err.sqlMessage, err.sql],
                'options': []
            };
            return res.status(200).send(response);
        }

        return res.status(200).send(rows1);
    });

});

app.get('/products/list', (req, res) => {

    const options = {
        query: 'SELECT * FROM products ORDER BY name ASC',
        location: 'EU',
    };

    bigquery.query(options, (err, rows1) => {
        if (err) {
            var response = {
                'code': 901,
                'type': 'error',
                'messages': [err.code, err.sqlMessage, err.sql],
                'options': []
            };
            return res.status(200).send(response);
        }

        return res.status(200).send(rows1);

    });
});

app.get('/categories/list', (req, res) => {

    const options = {
        query: 'SELECT * FROM categories ORDER BY title ASC',
        location: 'EU',
    };
 
    bigquery.query(options, (err, rows1) => {
        if (err) {
            var response = {
                'code': 901,
                'type': 'error',
                'messages': [err.code, err.sqlMessage, err.sql],
                'options': []
            };
            return res.status(200).send(response);
        }

        return res.status(200).send(rows1);

    });
});

app.post('/items/insert', (req, res) => {
    if (req.body.name.length > 22) {
        var response = {
            code: 305,
            type: 'error',
            messages: ['Produktname darf maximal 22 Zeichen enthalten!'],
            options: []
        };
        return res.status(200).send(response);
    }
    var values = req.body.name;
    if (typeof req.body.name === 'number') {

        const options = {
            query: 'SELECT * FROM products WHERE id = ' + req.body.name,
            location: 'EU',
        };

        bigquery.query(options, (err, rows1) => {
            if (err) {
                var response = {
                    'code': 901,
                    'type': 'error',
                    'messages': [err.code, err.sqlMessage, err.sql],
                    'options': []
                };
                return res.status(200).send(response);
            }
            if (rows1.length === 0) {
                var response = {
                    code: 302,
                    type: 'error',
                    messages: ['Angegebene Produkt-ID existiert nicht.'],
                    options: []
                };
                return res.status(200).send(response);
            } else {
                insertListItem(1, rows1[0].id, rows1[0].cat_id, req.body.quantity, req.body.urgent || 0, req.body.posId || 0, req.body.saleStart || undefined, req.body.saleEnd || undefined, req.body.force, function (success, response) {
                    if (!success) {
                        return res.status(200).send(response);
                    }
                    return res.status(200).send(response);
                });
            }
        });

    } else {

        const options = {
            query: 'SELECT * FROM products WHERE name = ' + req.body.name,
            location: 'EU',
        };

        bigquery.query(options, (err, rows1) => {
                if (err) {
                    var response = {
                        'code': 901,
                        'type': 'error',
                        'messages': [err.code, err.sqlMessage, err.sql],
                        'options': []
                    };
                    return res.status(200).send(rows1);
                }
                if (rows1.length === 0) {
                    insertProduct(req.body.name, req.body.catId, function (success, response) {
                        if (!success) {
                            return res.status(200).send(response);
                        }
                        insertListItem(1, response.productId, req.body.catId, req.body.quantity, req.body.urgent || 0, req.body.posId || 0, req.body.saleStart || undefined, req.body.saleEnd || undefined, req.body.force, function (success, response) {
                            if (!success) {
                                return res.status(200).send(response);
                            }
                            return res.status(200).send(response);
                        });
                    });
                } else {
                    insertListItem(1, rows1[0].id, rows1[0].cat_id, req.body.quantity, req.body.urgent || 0, req.body.posId || 0, req.body.saleStart || undefined, req.body.saleEnd || undefined, req.body.force, function (success, response) {
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
    updateListItem(req.body.listItemId, 1, req.body.quantity, req.body.posId || 0, req.body.urgent || 0, req.body.saleStart || undefined, req.body.saleEnd || undefined, function (success, response) {
        if (!success) {
            return res.status(200).send(response);
        }
        return res.status(200).send(response);
    });
});

app.post('/items/cart', (req, res) => {
    itemsCart(req.body.listItemId, 1, Number(req.body.cartStatus), function (success, response) {
        if (!success) {
            return res.status(200).send(response);
        }
        return res.status(200).send(response);
    });
});

app.post('/items/delete', (req, res) => {
    itemsDelete(req.body.listItemId, 1, function (success, response) {
        if (!success) {
            return res.status(200).send(response);
        }
        return res.status(200).send(response);
    });
});

app.post('/items/checkout', (req, res) => {
    itemsCheckout(req.body.listId, req.body.posId, function (success, response) {
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