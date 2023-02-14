const express = require('express');
const router = express.Router();
const productsService = require('../services/productsService');


router.get('/', async function (req, res) {
    try {
        let response = await productsService.get(req.query, {_id: 1, name: 1});
        res.send(response);
    } catch (err) {
        res.status(500).send(err);
    }
});

router.delete('/', async function (req, res) {
    try {
        let response = await productsService.remove(req.body.data);
        res.send(response);
    } catch (err) {
        res.status(500).send(err);
    }
});

router.put('/:id',  async function (req, res) {
    try {
        
        let response = await productsService.edit(req.params.id, req.body);
        res.send(response);
    } catch (err) {
        res.status(500).send(err);
    }
});


module.exports = router;