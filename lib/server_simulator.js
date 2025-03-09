const mqc = require('mysql_query_collector')
const http = require('http')
const dgram = require('dgram')
const net = require('net')

const traverse = require('object-traversal').traverse
const expr = require('node-expr')

//const m = require('data-matching');
//const sm = require('string-matching')

const deasync = require('deasync')

const _ = require('lodash')

const _collected_data = {}

var _interpolation_dict = null

//const traverseTemplate = require('traverse-template')

const eval_handlebars = (obj) => {
	function scan_and_interpolate({ parent, key, value, meta }) {
		if (typeof value === 'string') {
			var res = value.replace(/{{(.+?)}}/g, function(match, contents, offset, input_string) {
				//console.log("got ", contents)
				var e = new expr.Expr(contents)
				return e.test(_interpolation_dict)
			})
			parent[key] = res
		}
	}

	traverse(obj, scan_and_interpolate)
}

const types = require('mysql/lib/protocol/constants/types.js')

const querystringParse = require("querystring").parse

const nodeRedisProtocol = require('node-redis-protocol')
const redisProto = require('redis-proto')

var send_dbquery_reply = (conn, reply) => {
	//console.log("before", reply)
	eval_handlebars(reply)
	//console.log("after", reply)
	
	mqc.send_reply(conn, reply)
}

var send_init_db_reply = (conn, reply) => {
	eval_handlebars(reply)
	
	mqc.send_reply(conn, reply)
}

var send_http_reply = (res, reply) => {
	eval_handlebars(reply)

	res.writeHead(reply.status, reply.headers)
	if(_.some(reply.headers, (v,k) => v.toLowerCase() == 'application/json')) {
		res.end(JSON.stringify(reply.body))
	} else {
		res.end(reply.body)
	}
}

var send_udp_reply = (socket, rinfo, reply) => {
	eval_handlebars(reply)

	socket.send(reply.body, rinfo.port, rinfo.address, err => {
		if(err) {
			throw `Error when sending UDP packet ${reply} to ${rinfo.address}:${rinfo.port}: ${err}`
		}
	});
}

var send_redis_reply = (socket, reply) => {
	eval_handlebars(reply)

	var data = redisProto.encode(reply.body)
	//console.log("send_redis_reply: data=" + data)
	if(Array.isArray(reply.body)) {
		socket.write("*1\r\n", err => {
			if(err) {
				throw `Error when writing TCP packet ${data}: ${err}`
			}
			socket.write(data, err => {
				if(err) {
					throw `Error when writing TCP packet ${data}: ${err}`
				}
			});
		});
	} else {
		socket.write(data, err => {
			if(err) {
				throw `Error when writing TCP packet ${data}: ${err}`
			}
		});
	}
}

module.exports = {
	setup: (servers, event_cb, interpolation_dict) => {
		_interpolation_dict = interpolation_dict

		var mysql_servers = servers.filter(s => s.type == 'mysql')

		var http_servers = servers.filter(s => s.type == 'http')

		var udp_servers = servers.filter(s => s.type == 'udp')

		var redis_servers = servers.filter(s => s.type == 'redis')

		var ready_servers = 0;

		var update_ready_servers = c => {
			ready_servers += c
		}

		mqc.setup(mysql_servers, 
			() => {
				update_ready_servers(mysql_servers.length)
			},
			(conn, server, query) => {
				//console.debug(`Simulated MySQL server ${server_name} got query: ${query}`)

				var evt = {
					name: 'dbquery_request',
					server: server.name,
					query: query,
					conn: conn,
				}

				event_cb(evt)

				return null
			},
			(conn, server, database_name) => {
				var evt = {
					name: 'init_db_request',
					server: server.name,
					database_name: database_name,
					conn: conn,
				}

				event_cb(evt)

				//return {type: 'ok'}
				return null
			}
		)

		http_servers.forEach(server => {

			var post_http_request = (server, req, res, body, event_cb) => {
                req_clone = { ...req, headers: { ...req.headers } } // need to clone the object and force req.headers to be realized as new node versions changed it  use a getter function or proxy object
				req_clone.body = body

				var evt = {
					name: 'http_request',
					server: server.name,
					req: req_clone,
					res: res,
				}

				event_cb(evt)
			}

			var s = http.createServer((req, res) => {
				var data = ""
				var content_type = req.headers['content-type']

				req.on('data', function(chunk) {
					data += chunk.toString()
				})

				req.on('end', function() {
					var body
					switch(content_type) {
					case 'application/json':
						body = JSON.parse(data)
						break
					case 'application/x-www-form-urlencoded':
						body = querystringParse(data)
						break
					default:
						body = data
						break
					}
					post_http_request(server, req, res, body, event_cb)
				})
			})
			s.listen({
				host: server.address,
				port: server.port,
			})
			s.on('error', function (e) {
				throw e
			});
			s.on('listening', function (e) {
				//console.debug(`HTTP server ${server.name} created. Listening ${server.host}:${server.port}`)
				update_ready_servers(1)
			});
		})

		udp_servers.forEach(server => {
			var socket = dgram.createSocket('udp4');

			socket.on('error', err => {
				throw `server ${server.name} error:\n${err.stack}`
			});

			socket.on('message', (msg, rinfo) => {
				msg = msg.toString()
				//console.debug(`server got: ${msg} (${typeof msg}) from ${rinfo.address}:${rinfo.port}`)
				//
				event_cb({
					name: 'udp_request',
					server: server.name,
					msg: msg,
					rinfo: rinfo,
					socket: socket,
				})
			});

			socket.on('listening', () => {
				//console.debug(`UDP server ${server.name} listening ${server.host}:${server.port}`)
				update_ready_servers(1)
			});

			socket.bind(server.port, server.host);
		})

		redis_servers.forEach(server => {
			var s = net.createServer(socket => {
				var rp = new nodeRedisProtocol.ResponseParser()
				rp.on('response', response => {
					event_cb({
						name: 'redis_msg',
						server: server.name,
						msg: response, 
						socket: socket,
					})
				})

				socket.on('error', err => {
					throw `server ${server.name} error:\n${err.stack}`
				})

				socket.on('data', data => {
					rp.parse(data)
				})
			})

			s.listen(server.port, server.host)

			s.on('listening', () => {
				update_ready_servers(1)
			})
		})

		deasync.loopWhile(() => ready_servers != servers.length)
	},

	send_dbquery_reply: send_dbquery_reply,

	send_init_db_reply: send_init_db_reply,

	send_http_reply: send_http_reply,
	
	send_udp_reply: send_udp_reply,

	send_redis_reply: send_redis_reply,
}

