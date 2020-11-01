'use strict'

const {Pool} = require('pg')
const connectionPool = new Pool({
    connectionString: process.env.POSTGRESQL_DSN
})
const acceptedOperators = [
    '$eq',
    '$lt',
    '$lte',
    '$gt',
    '$gte',
    '$in',
    '$or',
    '$and'
]
const operatorsMap = {
    '$eq': '=',
    '$lt': '<',
    '$lte': '<=',
    '$gt': '>',
    '$gte': '>='
}

class Builder {
    constructor () {
        this.t_select = []
        this.t_keys = {} // untuk indexing berisi key {'user_name': '$1', 'user_email': '$2']
        this.t_where_and = []
        this.t_where_or = []
        this.t_join = []
        this.t_limit = 0
        this.t_offset = 0
    }

    /* only calling from internal class not public */
    /**
     * @param {string} key 
     * @description setting up the keys with spesific $index
     */
    addKey (key = '') {
        if (!this.t_keys[key]) this.t_keys[key] = `$${Object.keys(this.t_keys).length}`
    }

    getAllkeys () {
        return Object.keys(this.schemas)
    }

    /* START PREPARED FUNCTIONS CHAIN */
    /**
     * @description only used for prepared query
     */
    prepare () {
        this.use_prepare_statement = true
        return this
    }

    /**
     * @param {array} keys default new Array('*')
     * @description setup keys / fields which used to view
     */
    select (keys = ['*']) {
        this.is_select_query = true // untuk pengecekan dari builder
        const allKeys = this.getAllkeys()
        for (const k of keys) {
            if (k === '*' || allKeys.indexOf(k) > -1) {
                this.t_select.push(k)
            }
        }
        return this
    }

    /**
     * @param {string} key 
     * @param {*} value 
     * @description set "AND" statement variables
     */  
    where (object = {}) {
        const type = 'and'
        for (const key in object) {
            let value = object[key]
            if (key && value) {
                if (typeof value === 'object') {
                    // cek apakah sesuai dengan acceptedOperators
                    const opType = Object.keys(value)[0]
                    if (acceptedOperators.indexOf(opType) > -1) {
                        const operator = operatorsMap[opType]
                        if (operator) {
                            value = value[Object.keys(value)[0]]
                            this.t_where_and.push({
                                [key]: { type, operator, value }
                            })
                        } else if (opType === '$in') {
                            value = value[Object.keys(value)[0]]
                            this.t_where_and.push({
                                [key]: { type, operator: 'IN', value }
                            })
                        }
                    }
                } else {
                    this.t_where_and.push({
                        [key]: { type, operator: '=', value }
                    })
                }
            }
        }
        return this
    }

    /**
     * @param {object} object default {}
     * @description set "OR" statement variables
     */
    orWhere (object = {}) {
        const type = 'or'
        let operator = '='
        for (const key in object) {
            let value = object[key]
            if (key && value) {
                if (typeof value === 'object') {
                    // cek apakah sesuai dengan acceptedOperators
                    const opType = Object.keys(value)[0]
                    if (acceptedOperators.indexOf(opType) > -1) {
                        operator = operatorsMap[opType]
                        if (operator) {
                            value = value[Object.keys(value)[0]]
                            this.t_where_or.push({
                                [key]: { type, operator, value }
                            })
                        }
                    }
                } else {
                    this.t_where_or.push({
                        [key]: { type, operator, value }
                    })
                }
            }
        }
        return this
    }

    /**
     * @param {int} limit
     * @description set limit of query
     */
    limit (limit = 0) {
        this.t_limit = limit
        return this
    }

    /**
     * @param {int} offset
     * @description set offset
     */
    offset (offset = 0) {
        this.t_offset = offset
        return this
    }

    /* END OF PREPARED FUNCTIONS CHAIN */
    /**
     * @description used for build all prepared statement object and generating to sql statement
     */
    buildQuery () {
        const sql = []
        const values = []
        if (this.is_select_query) {
            const fields = this.t_select.join(', ')
            sql.push(`SELECT ${fields} FROM ${this.tableName}`)
        } else if (this.is_update_query) {
            sql.push(`UPDATE ${this.tableName} SET`)
            const {stringFieldAndValue, values: v} = this.getFieldAndValues({})
            if (Array.isArray(stringFieldAndValue)) sql.push(...(stringFieldAndValue || []))
            if (Array.isArray(v)) values.push(...(v || []))
        }
        const {where, mapValueSequence} = this.generateCriterias({initValues: values})
        if (Array.isArray(where)) sql.push(...(where || []))
        if (Array.isArray(mapValueSequence)) values.push(...(mapValueSequence || []))
        if (this.is_select_query || this.is_update_query) {
            const limitOffsets = this.getLimitAndOffset()
            if (Array.isArray(limitOffsets)) sql.push(...(limitOffsets || []))
        }
        return {sql, values: mapValueSequence}
    }

    /**
     * @param {object} param0 default { initValues }
     * @description get field and values from object and generate to string with template number $n
     */
    getFieldAndValues ({ initValues }) {
        if (!initValues) initValues = []
        const val = [...initValues]
        let stringFieldAndValue = []
        if (!this.field_value_object) return stringFieldAndValue
        const initsize = initValues.length
        for (const fvo in this.field_value_object) {
            const n = val.length + 1
            if (n - initsize > 1) stringFieldAndValue.push(',')
            stringFieldAndValue.push(`${fvo} = $${n}`)
            val.push(this.field_value_object[fvo])
        }
        debugger
        return {stringFieldAndValue, values: val}
    }

    /**
     * @description setup limit and offset if defined
     */
    getLimitAndOffset () {
        let sql = []
        if (this.t_limit) sql.push(`LIMIT ${this.t_limit}`)
        if (this.t_offset) sql.push(`OFFSET ${this.t_offset}`)
        return sql
    }

    /**
     * @description generate criteria from object criterias
     * @param {object} param0 default { initvalues = [] }
     */
    generateCriterias ({initValues = []}) {
        let where = []
        const wAND = this.t_where_and
        const wOR = this.t_where_or
        const initSize = initValues.length
        let sequence = initSize + 1
        const kurung = ['(', ')']
        const wANDsize = wAND.length
        const wORsize = wOR.length
        let mapValueSequence = [...initValues]
        if (wANDsize > 0) {
            if (sequence - initSize === 1) where.push('WHERE')
            let s = 1
            for (const obj of wAND) {
                const key = Object.keys(obj)[0]
                const type = obj[key]['type'].toUpperCase()
                const val = obj[key]['value']
                const op = obj[key]['operator']
                if (sequence - initSize > 1) where.push(type)
                const k0 = '' // (s === 1 ? kurung[0] : '')
                const k1 = '' // (s === wANDsize ? kurung[1] : '')
                if (op === 'IN') {
                    where.push(`${k0}${key} ${op} ($${sequence})${k1}`)
                    mapValueSequence.push(val.join())
                } else {
                    where.push(`${k0}${key} ${op} $${sequence}${k1}`)
                    mapValueSequence.push(val)
                }
                sequence += 1
                s += 1
            }
        }
        if (wORsize > 0) {
            if (sequence - initSize === 1) where.push('WHERE')
            let s = 1
            for (const obj of wOR) {
                const key = Object.keys(obj)[0]
                const type = obj[key]['type'].toUpperCase()
                const val = obj[key]['value']
                const op = obj[key]['operator']
                // if (s === 1) where.push('(')
                if (sequence - initSize > 1) where.push(type)
                const k1 = '' //(s === wORsize ? kurung[1] : '')
                if (op === 'IN') {
                    where.push(`${key} ${op} ($${sequence})${k1}`)
                } else {
                    where.push(`${key} ${op} $${sequence}${k1}`)
                }
                mapValueSequence.push(val)
                sequence += 1
                s += 1
            }
        }
        return { where, mapValueSequence }
    }

    /* STANDALONE FUNCTIONS BUT STILL USING PREPARED STATEMENT */
    /**
     * @description standalone function
     * @param {object} criteria 
     * @param {object} updates 
     * @param {object} options 
     */
    async updateOne (criteria = {}, updates = {}, options = {}) {
        try {
            const selectStatement = this
                .prepare()
                .select(['*'])
                .where(criteria) // preparing where statement for selecting data
                .buildQuery()
            this.is_update_query = true // setup options to update query
            console.log({selectStatement})
            if (Object.keys(updates).length > 0) {
                this.field_value_object = {}
                const keys = this.getAllkeys()
                for (const f in updates) {
                    console.log('f:', f)
                    if (keys.indexOf(f) > -1) {
                        this.field_value_object[f] = updates[f]
                    }
                }
            }
            return this
        } catch (err) {
            throw err
        }
    }

    async findAndUpdate(criteria = {}, update = {}, options = {}) {

    }

    async insertOne (data = {}) {
        try {
            let keys = []
            let values = []
            let preparedMap = []
            let mapValue = 1
            for (const key in data) {
                keys.push(key)
                values.push(data[key])
                preparedMap.push(`$${mapValue}`)
                mapValue += 1
            }
            const sql = `INSERT INTO ${this.tableName} (${keys.join()}) values (${preparedMap.join(',')})`
            await this.fetch(sql, values)
        } catch (err) {
            throw err
        }
    }

    async deleteOne (criterias = {}) {
        try {
            if (Object.keys(criterias).length > 0) this.where(criterias)
        } catch (err) {
            throw err
        }
    }

    async rawQuery (sql = '', values = []) {
        try {
            const data = await this.fetch(sql, values)
            return data
        } catch (err) {
            throw err
        }
    }
}

class BaseModel extends Builder {
    constructor () {
        super()
        this.whereClauses = []
        this.values = []
    }

    async execute () {
        try {
            let {sql, values } = this.buildQuery()
            if (typeof sql !== 'string') sql = sql.join(' ')
            console.logger('running query:', {sql, values})
            const q = await connectionPool.query(sql, values)
            await connectionPool.end()
            return q
        } catch (err) {
            console.error(err)
            return null
        }
    }

    async findOne (criteria, options = {}) {
        try {
            for (const field in criteria) {
                this.where(field, criteria[field])
            }
            const sql = `SELECT * FROM ${this.tableName} ${this.whereClauses.join(' ')} LIMIT 1`
            const q = await this.fetch(sql, this.values)
            return this.getResult(q,'rows[0]', {})
        } catch (err) {
            throw err
        }
    }

    async findBy (criteria, options = {}) {
        try {
            for (const field in criteria) {
                this.where(field, criteria[field])
            }
            const sql = `SELECT * FROM ${this.tableName} ${this.whereClauses.join(' ')}`
            const q = await this.fetch(sql, this.values)
            return this.getResult(q,'rows', [])
        } catch (err) {
            throw err
        }
    }
}
module.exports = BaseModel