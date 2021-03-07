'use strict'

const {Pool} = require('pg')
const connectionPool = new Pool({
    connectionString: process.env.POSTGRESQL_DSN
})
const md5 = require('md5')
const {result} = require('lodash')
const acceptedOperators = [
    '$eq',
    '$ne',
    '$lt',
    '$lte',
    '$gt',
    '$gte',
    '$in',
    '$or',
    '$and',
    '$like'
]
const operatorsMap = {
    '$eq': '=',
    '$ne': '<>',
    '$lt': '<',
    '$lte': '<=',
    '$gt': '>',
    '$gte': '>=',
    '$in': 'IN',
    '$like': 'LIKE'
}

class Builder {
    constructor () {
        this.reset()
    }

    /* RESET ALL VALUES */
    /**
     * @function reset
     * @description resetting for all variables
     */
    reset () {
        this.use_prepare_statement = false
        this.is_select_query = false
        this.is_update_query = false
        this.field_value_object = {}
        this.t_select = []
        this.t_keys = {} // untuk indexing berisi key {'user_name': '$1', 'user_email': '$2']
        this.t_where_and = []
        this.t_where_or = []
        this.t_join = []
        this.t_sort = []
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

    /**
     * @function getAllkeys
     * @description getting valid keys in array format
     */
    getAllkeys () {
        return Object.keys(this.schemas)
    }

    /* START PREPARED FUNCTIONS CHAIN */
    /**
     * @description only used for prepared query
     */
    prepare (type = 'select') {
        this.reset() // set to default value
        this.use_prepare_statement = true
        if (type === 'select') this.is_select_query = true
        else if (type === 'update') this.is_update_query = true
        else if (type === 'deleteone') this.is_deleteone_query = true
        else if (type === 'remove') this.is_remove_query = true
        return this
    }
    /**
     * @function from
     * @param {*} tableName 
     */
    from (tableName = '') {
        this.fromTable = tableName
        return this
    }

    /**
     * @param {array} keys default new Array('*')
     * @description setup keys / fields which used to view
     */
    select (keys, noValidation = false) {
        this.is_select_query = true // untuk pengecekan dari builder
        if (this.t_select.length === 0 && !keys) this.t_select.push('*')
        if (keys) {
            const allKeys = this.getAllkeys()
            for (const k of keys) {
                const key = k.split('.')[1] || k.split('.')[0]
                if (k === '*' || allKeys.indexOf(key) > -1 || noValidation) {
                    this.t_select.push(k)
                }
            }
        }
        return this
    }

    /**
     * @param {object} object
     * @param {boolean} sqlReturned default false
     * @description set "AND" statement variables
     */  
    where (object = {}, sqlReturned = false) {
        if (typeof object !== 'object') throw new Error('parameter pertama harus object {key: value}')
        const type = 'and'
        for (const key in object) {
            let value = object[key]
            if (key && (value || value === 0)) {
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
        if (sqlReturned) {
            const {sql, values} = this.generateCriterias({})
            return {sql: sql.join(' '), values}
        }
        return this
    }

    /**
     * @param {object} object default {}
     * @description set "OR" statement variables
     */
    orWhere (object = {}) {
        const type = 'or'
        for (const key in object) {
            let operator = '='
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

    /**
     * @function join
     * @param {*} type 
     * @param {*} fromTable 
     * @param {*} args 
     */
    join (type = 'left', fromTable, args) {
        if (!fromTable) throw new Error('Required second paramater')
        if (!args) throw new Error('Required third paramater')
        this.t_join.push({type, fromTable, args})
        return this
    }

    /**
     * @function sort
     * @param {*} key 
     * @param {*} dir 
     */
    sort (key, dir = 'DESC') {
        if (!key) throw new Error('Key Is Required For Sorting')
        this.t_sort.push({key, dir})
        return this
    }

    /* END OF PREPARED FUNCTIONS CHAIN */
    /**
     * @description used for build all prepared statement object and generating to sql statement
     */
    buildQuery (options = {}) {
        let { initValues } = options
        const sql = []
        let values = !initValues ? [] : initValues
        if (this.is_select_query) {
            const fields = this.t_select.join(', ')
            sql.push(`SELECT ${fields} FROM ${this.fromTable || this.tableName}`)
            if (this.t_join.length > 0) {
                for (const j of this.t_join) {
                    sql.push(`${j.type.toUpperCase()} JOIN ${j.fromTable}`)
                    sql.push(`ON ${j.args}`)
                }
            }
        } else if (this.is_update_query) {
            sql.push(`UPDATE ${this.tableName} SET`)
            const {stringFieldAndValue, values: newValues1} = this.getFieldAndValues({initValues})
            if (Array.isArray(stringFieldAndValue)) sql.push(...(stringFieldAndValue || []))
            values = newValues1
            if (this.fromTable) sql.push(`FROM ${this.fromTable}`)
            // untuk updateone tidak di masukkan where disini, krn harus select one dlu lalu di update
            // untuk update yg many, tidak ada masalah menggunakan .where()
        } else if (this.is_deleteone_query || this.is_remove_query) {
            sql.push(`DELETE FROM ${this.tableName}`)
        }
        const {sql: sqlCriteria, values: newValues2} = this.generateCriterias({initValues: values})
        if (Array.isArray(sqlCriteria)) sql.push(...sqlCriteria)
        if (this.t_sort) {
            if (this.t_sort.length > 0) sql.push('ORDER BY')
            for (const s of this.t_sort) {
                const sortKey = s.key
                const sortDir = s.dir
                sql.push(`${sortKey} ${sortDir}`)
            }
        }
        if (this.is_select_query || this.is_update_query) {
            const limitOffsets = this.getLimitAndOffset()
            if (Array.isArray(limitOffsets)) sql.push(...(limitOffsets || []))
        }
        // biarkan sql bertype Array krn ada beberapa yang dibutuhkan untuk menambah item spt updateOne
        return {sql, values: newValues2}
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
        return {stringFieldAndValue, values: val}
    }

    /**
     * @function data
     * @param {Object} obj 
     */
    data (obj = {}) {
        this.field_value_object = {}
        if (Object.keys(obj).length > 0) {
            const keys = this.getAllkeys()
            for (const f in obj) {
                if (keys.indexOf(f) > -1) {
                    this.field_value_object[f] = obj[f]
                }
            }
        }
        return this
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
        let sql = []
        const wAND = this.t_where_and
        const wOR = this.t_where_or
        const initSize = initValues.length
        let sequence = initSize + 1
        const wANDsize = wAND.length
        const wORsize = wOR.length
        let newValues = [...initValues]
        if (wANDsize > 0) {
            if (sequence - initSize === 1) sql.push('WHERE')
            let s = 1
            for (const obj of wAND) {
                const key = Object.keys(obj)[0]
                const type = obj[key]['type'].toUpperCase()
                const val = obj[key]['value']
                const op = obj[key]['operator']
                if (sequence - initSize > 1) sql.push(type)
                const k0 = '' // (s === 1 ? kurung[0] : '')
                const k1 = '' // (s === wANDsize ? kurung[1] : '')
                if (op === 'IN') {
                    let allSequence = ''
                    for (const inSeq of val) {
                        newValues.push(inSeq)
                        if (allSequence.length > 0) allSequence += ','
                        allSequence += `$${sequence}`
                        sequence += 1
                    }
                    sql.push(`${k0}${key} ${op} (${allSequence})${k1}`)
                } else if (op === 'LIKE') {
                    sql.push(`${k0}LOWER(${key}) ${op} LOWER($${sequence})${k1}`)
                    /newValues.push(val)
                    sequence += 1
                } else {
                    sql.push(`${k0}${key} ${op} $${sequence}${k1}`)
                    newValues.push(val)
                    sequence += 1
                }
                s += 1
            }
        }
        if (wORsize > 0) {
            if (sequence - initSize === 1) sql.push('WHERE')
            let s = 1
            for (const obj of wOR) {
                const key = Object.keys(obj)[0]
                const type = obj[key]['type'].toUpperCase()
                const val = obj[key]['value']
                const op = obj[key]['operator'] || '='
                // if (s === 1) sql.push('(')
                if (sequence - initSize > 1) sql.push(type)
                const k1 = '' //(s === wORsize ? kurung[1] : '')
                if (!op) {
                    console.error(obj[key])
                } else if (op === 'IN') {
                    sql.push(`${key} ${op} ($${sequence})${k1}`)
                } else {
                    sql.push(`${key} ${op} $${sequence}${k1}`)
                }
                newValues.push(val)
                sequence += 1
                s += 1
            }
        }
        return { sql, values: newValues }
    }
}

class BaseModel extends Builder {
    constructor () {
        super()
        this.whereClauses = []
        this.values = []
    }

    async execute (sql, values, returnObject = false) {
        try {
            if (!sql) {
                const builder = this.buildQuery()
                sql = builder.sql
                values = builder.values
            }
            if (typeof sql !== 'string') sql = sql.join(' ')
            console.logger('running query:', {sql, values})
            const queryResult = await connectionPool.query(sql, values)
            return { queryResult, raw: { sql, values } }
        } catch (err) {
            throw new Error(err.message)
        }
    }

    /* STANDALONE FUNCTIONS BUT STILL USING PREPARED STATEMENT */
    /**
     * @description standalone function
     * @param {object} criteria 
     * @param {object} updates 
     * @param {object} options {upsert}
     */
    async updateOne (criteria = {}, updates = {}, options = {}) {
        try {
            if (options.upsert) {
                const data = await this.findOneAndUpdate(criteria, updates, options)
                return data
            }
            const dataupdate = updates['$set'] || updates
            const sqlupdate = []
            const {sql: sqlFrom, values: valFrom} = this
                .prepare('select')
                .select(['_id'])
                .limit(1)
                .where(criteria) // preparing where statement for selecting data
                .buildQuery({}) // values(type Array) akan melanjutkan urutan sesuai size yg sudah didefinisikan
            sqlupdate.push(`with cte as`)
            sqlupdate.push('(')
            if (Array.isArray(sqlFrom)) sqlupdate.push(...sqlFrom)
            sqlupdate.push(')')
            const {sql, values} = this
                .prepare('update')
                .data(dataupdate)
                .from('cte') // krn menggunakan alias
                // .where({
                //     'cte.id': `${this.tableName}.id`
                // })
                .buildQuery({initValues: valFrom})
            if (Array.isArray(sql)) sqlupdate.push(...sql)
            sqlupdate.push(`WHERE cte._id = ${this.tableName}._id`)
            const data = await this.execute(sqlupdate, values)
            return data
        } catch (err) {
            throw err
        }
    }

    async findOneAndUpdate(criteria = {}, update = {}, options = {}) {
        try {
            const {queryResult} = await this.findOne(criteria)
            let actions = {}
            let newData = {...queryResult}
            if (!queryResult.id) {
                if (options.upsert === true) {
                    const n = {...update['$set'], ...update['$setOnInsert']}
                    await this.insertOne(n)
                    actions['selectedAction'] = 'insert'
                    newData = {...newData, ...n}
                } else {
                    console.logger('Data not found and not updated!')
                }
            } else {
                if (!update['$set']) throw new Error('Update need $set or $setOneInsert object')
                await this.update({id: queryResult.id}, update['$set'])
                actions['selectedAction'] = 'update'
                newData = {...newData, ...update['$set']}
            }
            return {data: newData, actions}
        } catch (err) {
            throw err
        }
    }

    async update (criteria = {}, data = {}) {
        try {
            const d = await this
                .prepare('update')
                .where(criteria)
                .data(data)
                .execute()
            return d
        } catch (err) {
            throw err
        }
    }

    async insertOne (data = {}) {
        try {
            let keys = []
            let values = []
            let preparedMap = []
            let mapValue = 1
            if (!data['_id']) data['_id'] = md5(`${this.tableName}_${new Date().getTime()}`)
            for (const key in data) {
                keys.push(key)
                values.push(data[key])
                preparedMap.push(`$${mapValue}`)
                mapValue += 1
            }
            const sql = `INSERT INTO ${this.tableName} (${keys.join()}) values (${preparedMap.join(',')}) RETURNING id`
            const {queryResult} = await this.execute(sql, values)
            return {
                id: result(queryResult, 'rows[0].id', null),
                _id: data._id
            }
        } catch (err) {
            throw err
        }
    }

    async deleteOne (criterias = {}) {
        try {
            if (Object.keys(criterias).length === 0) throw new Error('DeleteOne Need atlease One criteria')
            return await this
                .prepare('deleteone')
                .where(criterias)
                .limit(1)
                .execute()
        } catch (err) {
            throw err
        }
    }

    async remove (criterias = {}) {
        try {
            if (Object.keys(criterias).length === 0) throw new Error('DeleteOne Need atlease One criteria')
            await this
                .prepare('remove')
                .where(criterias)
                .execute()
        } catch (err) {
            throw err
        }
    }

    async rawQuery (sql = '', values = []) {
        try {
            const data = await this.execute(sql, values)
            return data
        } catch (err) {
            throw err
        }
    }

    async findOne (criteria = {}, options = {}) {
        try {
            const isNoValidation = result(options, 'join', []).length > 0
            let q = this
                .select(options.select, isNoValidation)
            if (Object.keys(criteria).length > 0) q = q.where(criteria)
            if (options.join) {
                for (const j of options.join) {
                    q = q.join(j.type, j.from, j.args)
                }
            }
            q = q.limit(1)
            const {queryResult, raw} = await q.execute()
            return {queryResult: result(queryResult, 'rows[0]', {}), raw}
        } catch (err) {
            throw err
        }
    }

    async findOneAndDelete (criteria) {
        try {
            const {queryResult} = await this.findOne(criteria)
            await this.deleteOne(criteria)
            return queryResult
        } catch (err) {
            throw err
        }
    }

    async findAll (criteria = {}, options = {}) {
        try {
            const isNoValidation = result(options, 'join', []).length > 0
            let q = this
                .select(options.select || ['*'], isNoValidation)
            if (Object.keys(criteria).length > 0) q = q.where(criteria)
            if (options.join) {
                for (const j of options.join) {
                    q = q.join(j.type, j.from, j.args)
                }
            }
            const {queryResult, raw} = await q.execute()
            return {
                data: result(queryResult, 'rows', []),
                raw
            }
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
            const q = await this.execute(sql, this.values)
            return result(q,'rows', [])
        } catch (err) {
            throw err
        }
    }

    async count (criteria = {}) {
        try {
            const {sql, values} = this
                .prepare('select')
                .select(['COUNT(_id)'], true)
                .where(criteria)
                .buildQuery()
            const {queryResult} = await this.execute(sql, values)
            return parseInt(result(queryResult, 'rows[0].count', 0))
        } catch (err) {
            throw err
        }
    }
}
module.exports = BaseModel