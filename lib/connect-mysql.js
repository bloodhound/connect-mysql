/*
 Author: Nathan LaFreniere <nlf@andyet.net>
 Homepage: https://github.com/nlf/connect-mysql
 
 MySQL pool support: Anton Skshidlevsky <meefik@gmail.com>
 Homepage: https://github.com/meefik/connect-mysql
 */

module.exports = function (connect) {
    var Store = connect.session.Store
      , TableName = 'sessions'
      , KeyName = 'sid'
      , DataName = 'session'
      , ExpiresName = 'expires';

    function MySQLStore(options) {
        var cleanup = true;
        Store.call(this, options);
        if (options.hasOwnProperty('cleanup')) cleanup = options.cleanup;
        if (options.hasOwnProperty('table')) TableName = options.table;
        if (options.hasOwnProperty('key')) KeyName = options.key;
        if (options.hasOwnProperty('data')) DataName = options.data;
        if (options.hasOwnProperty('expires')) ExpiresName = options.expires;

        var query = function(connection, pool) {
            connection.query('CREATE TABLE IF NOT EXISTS `' + TableName + '` (`' + KeyName + '` VARCHAR(255) NOT NULL, `' + DataName + '` TEXT NOT NULL, `' + ExpiresName + '` INT, PRIMARY KEY (`' + KeyName + '`) )', function (err) {
                if (err) throw err;
                if (cleanup) {
                    connection.query('CREATE EVENT IF NOT EXISTS `sess_cleanup` ON SCHEDULE EVERY 15 MINUTE DO DELETE FROM `' + TableName + '` WHERE `' + ExpiresName + '` < UNIX_TIMESTAMP()');
                    connection.query('SET GLOBAL event_scheduler = 1');
                }
                if (pool) connection.release();
            });
        }

        this.pool = options.client.config.connectionConfig ? true : false;
        this.mysql = options.client;

        if (this.pool) {
            this.mysql.getConnection(function(err, connection) {
                if (err) throw err;
                query(connection, true);
            });
        } else {
            query(this.mysql, false);
        }
    }

    MySQLStore.prototype.__proto__ = Store.prototype;

    MySQLStore.prototype.get = function (sid, callback) {
        var query = function(connection, pool) {
            connection.query('SELECT `' + DataName + '` FROM `' + TableName + '` WHERE `' + KeyName + '` = ?', [sid], function (err, result) {
                if (result && result[0] && result[0][DataName]) {
                    callback(null, JSON.parse(result[0][DataName]));
                } else {
                    callback(err);
                }
                if (pool) connection.release();
            }).on('error', function (err) {
                    callback(err);
                });
        }
        if (this.pool) {
            this.mysql.getConnection(function(err, connection) {
                if (err) callback(err);
                else query(connection, true);
            });
        } else {
            query(this.mysql, false);
        }
    };

    MySQLStore.prototype.set = function (sid, session, callback) {
        var expires = new Date(session.cookie.expires).getTime() / 1000;
        session = JSON.stringify(session);
        var query = function(connection, pool) {
            connection.query('INSERT INTO `' + TableName + '` (`' + KeyName + '`, `' + DataName + '`, `' + ExpiresName + '`) VALUES(?, ?, ?) ON DUPLICATE KEY UPDATE `' + DataName + '` = ?, `' + ExpiresName + '` = ?', [sid, session, expires, session, expires], function (err) {
                callback(err);
                if (pool) connection.release();
            });
        }
        if (this.pool) {
            this.mysql.getConnection(function(err, connection) {
                if (err) callback(err);
                else query(connection, true);
            });
        } else {
            query(this.mysql, false);
        }
    };

    MySQLStore.prototype.destroy = function (sid, callback) {
        var query = function(connection, pool) {
            connection.query('DELETE FROM `' + TableName + '` WHERE `' + KeyName + '` = ?', [sid], function (err) {
                callback(err);
                if (pool) connection.release();
            });
        }
        if (this.pool) {
            this.mysql.getConnection(function(err, connection) {
                if (err) callback(err);
                else query(connection, true);
            });
        } else {
            query(this.mysql, false);
        }
    };

    return MySQLStore;
};
