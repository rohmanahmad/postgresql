'use strict'

const PostgresORM = require('postgresql-orm')

class ExampleModel extends PostgresORM {
    get tableName () {
        return 'table_name_terserah'
    }

    get connection () {
        return 'pg'
    }

    get schemas () {
        return {
            id: {
                type: Number,
                stringType: 'int4',
                size: 0,
                isNullable: false
            },
            user_email: {
                type: String,
                stringType: 'bpchar',
                size: 50,
                isNullable: false
            },
            user_password: {
                type: String,
                stringType: 'text',
                size: 0,
                isNullable: false
            },
            user_phonenumber: {
                type: String,
                stringType: 'bpchar',
                size: 20,
                isNullable: false
            },
            is_active: {
                type: Boolean,
                stringType: 'bool',
                size: 0,
                isNullable: false
            },
            is_blocked: {
                type: Boolean,
                stringType: 'bool',
                size: 0,
                isNullable: false
            },
            ukm_id: {
                type: Number,
                stringType: 'int4',
                size: 0,
                isNullable: false
            }, // jika user tsb juga sebagai mitra, maka ukm id ada isinya
            customer_id: {
                type: Number,
                stringType: 'int4',
                size: 0,
                isNullable: false
            },
            created_at: {
                type: Date,
                stringType: 'timestamp',
                size: 0,
                isNullable: false
            },
            updated_at: {
                type: Date,
                stringType: 'timestamp',
                size: 0,
                isNullable: false
            }
        }
    }

    get index () {
        return {
            primary: {
                keys: {id: -1},
                uniq: true
            },
            user_email: { // digunakan untuk getInformation
                keys: {user_email: 1},
                uniq: true
            },
            user_phonenumber: { // digunakan untuk getInformation by phonenumber
                keys: {user_phonenumber: 1},
                uniq: true
            },
            ukm_id: { // digunakan untuk pencarian by ukmid
                keys: {ukm_id: 1},
                uniq: true
            },
            created_date: { // untuk sorting kebanyakan DESC
                keys: {created_at: -1},
                uniq: false
            }
        }
    }
}

module.exports = ExampleModel 