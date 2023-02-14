const productsDao = require('../daos/productsDao');
const appLogger = require('../logging/appLogger');

async function get(query, projection) {
    try {
        let resp = await productsDao.getBy(query, projection);
        return Promise.resolve(resp);
    } catch (err) {
        appLogger.error(JSON.stringify(err));
        return Promise.reject(err);
    }
}

async function remove(query) {
    try {
        let resp = await productsDao.removeBy(query);
        return Promise.resolve(resp);
    } catch (err) {
        appLogger.error(JSON.stringify(err));
        return Promise.reject(err);
    }
}

async function edit(query, detailsToUpdate) {
    try {
        let resp = await productsDao.updateOne(query, "SET", detailsToUpdate);
        return Promise.resolve(resp);
    } catch (err) {
        appLogger.error(JSON.stringify(err));
        return Promise.reject(err);
    }
}

module.exports.edit = edit;
module.exports.remove = remove;
module.exports.get = get;