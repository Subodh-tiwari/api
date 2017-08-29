"use strict"

import * as Hapi from 'hapi';
const server = new Hapi.Server();

import * as amqp from 'amqplib/callback_api';

import * as Knex from 'knex';
const knex = Knex({
  client : "pg",
  connection : {
      host : "localhost",
      user : "root",
      password : "prabodh@69",
      database : "node_postgres"
  },
  debug : true
});

import * as Redis from 'ioredis';

const redis = new Redis();

server.connection({port : 3000, host : "localhost"});

/////////////////////////////// START -- SAVE DATA IN REDIS, RABBITMQ

server.route({
    method : "GET",
    path : "/",
    handler :function(request, reply) {
        const username = request.query.username;
        const password = request.query.password;

        const key = `subodh:${String(Date.now())}`;
        const value = {
            username,
            password
        }

        //////// -----amqplib sender START----- ////////

        amqp.connect('amqp://localhost', function(err, conn) {
          conn.createChannel(function(err, ch) {
            var q = 'hello';

            ch.assertQueue(q, {durable: false});
            ch.sendToQueue(q, new Buffer(key));
            console.log(` [x] Sent '${key}'`);
          });
          // setTimeout(function() { conn.close(); process.exit(0) }, 500);
        });

        //////// -----amqplib sender END---- ////////
        redis.set(key, JSON.stringify(value));
        reply(`Hello ${key}, this is your password ${JSON.stringify(value)}`);
    }
});

////////////////////////// END --- SAVE DATA IN REDIS, RABBITMQ

//////////////////////////// START ---- SAVE DATA IN POSTGRES

server.route({
    method : "GET",
    path : "/set",
    handler : function(request, reply) {

        //////// -----amqplib receiver START----- ////////

        amqp.connect('amqp://localhost', function(err, conn) {
          conn.createChannel(function(err, ch) {
            var q = 'hello';

            ch.assertQueue(q, {durable: false});
            console.log(" [*] Waiting for messages in %s. To exit press CTRL+C", q);
            ch.consume(q, function(msg) {
              console.log(" [x] Received %s", msg.content.toString());
              redis.get(msg.content.toString(), function(err, result) {
                  if(err) {
                      throw err;
                  } else {
                      const data = JSON.parse(result);
                      console.log(`${data.username}, ${data.password}`);

                      knex.schema.createTable('subodh', function(table) {
                          table.increments('id');
                          table.string('username');
                          table.string('password');
                      });
                      knex.schema.then(function() {
                          knex.table('subodh').insert({username : data.username, password : data.password});
                      });
                      knex.schema.map(function(row) {
                          console.log(row);
                      });
                      knex.schema.catch(function(e) {
                          console.error(e);
                      });
                  }
              });
            }, {noAck: true});
          });
        });

        //////// -----amqplib receiver END----- ////////

    }
});

////////////////////////// END --- SAVE DATA IN POSTGRES

server.start((err) => {

    if(err) {
        throw err;
    }

    console.log(`server running at : ${server.info.uri}`);
});
