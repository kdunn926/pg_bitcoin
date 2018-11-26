drop type bitcoin_transaction cascade ;
drop type bitcoin_transaction_input ;
drop type bitcoin_transaction_input_outpoint ;
drop type bitcoin_transaction_output ;
drop type bitcoin_transaction_witness ;

create type bitcoin_transaction_input_outpoint as (
	hash char(64),
	index bigint
);

create type bitcoin_transaction_input as (
	outpoint bitcoin_transaction_input_outpoint,
	script char(46),
	sequence bigint
);

create type bitcoin_transaction_output as (
	value bigint,
	script text	
);

create type bitcoin_transaction_witness as (
	number bigint,
	"scriptCode" char(212)
);

create type bitcoin_transaction as (
	ins bitcoin_transaction_input[],
	outs bitcoin_transaction_output[],
	version int,
	marker int,
	flag int,
	witness bitcoin_transaction_witness[],
	locktime bigint
);

create or replace function btcd_bitcoin_raw_transaction(arg_url text, arg_raw_transaction text default '9d609c57efa02ec1e897e467298f1db8b4a9aa2ac611c4684aa24e243da251e5')
returns bitcoin_transaction
as $$

from os import environ

# The install procedure for these packages depends on how  pl/Python 
# support was compiled for Postgres (e.g. which interpreter is used)
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException
from cryptos import deserialize as tx_deserialize

url = None

if arg_url is not None:
    # Use a "http://user:password@host:port" URL string from the argument list
    url = arg_url

else:
    # Postgres must be started with the 
    # following variables in the environment, 
    # each prefixed by btcd_rpc_ (e.g. btcd_rpc_host)
    rpc_vars = ['host', 'port', 'user', 'password']

    required_vars = set('btcd_rpc_' + v for v in rpc_vars)
    defined_vars = set(environ).intersection(required_vars)
    missing_vars = required_vars.difference(defined_vars)

    if missing_vars and not arg_url:
        message = "The following " if len(defined_vars) else "No "
        message += "environment variables were detected..."
        plpy.notice(message)
        plpy.notice(defined_vars if len(defined_vars) else '')
        plpy.notice("")
        plpy.notice("Please set these environment variables in the postgres user environment and restart:")
        plpy.notice("    '" + "', '".join(missing_vars) + "'")
        plpy.notice("")
        plpy.error("Missing required btcd environment variables or URL argument")

    rpc_config = { k: environ.get('btcd_rpc_' + k) for k in rpc_vars }

    url = "http://{user}:{password}@{host}:{port}".format(**rpc_config)


try:
    rpc_connection = AuthServiceProxy(url)
    node_info = rpc_connection.getblockchaininfo()
    plpy.info("Current block height: {}".format(node_info.get('headers')))

except JSONRPCException as e:
    plpy.error(e.message)

tx = tx_deserialize(rpc_connection.getrawtransaction(arg_raw_transaction))

#     Note: the following two statements could be used to 
#     flatten the transaction inputs but it was interesting to
#     see how far nesting Postgres user-defined types would go
#
# unnestIns = lambda d: { 'script': d.get('script'), 
#                        'sequence': d.get('sequence'), 
#                        'hash': d.get('outpoint').get('hash'),
#                        'index': d.get('outpoint').get('index') }
#
# tx['ins'] = [unnestIns(i) for i in tx['ins']]

# Use a template for compatibility with earlier transactions
txTemplate = {
  'ins': None,
  'outs': None,
  'version': None,
  'marker': None,
  'flag': None,
  'witness': None,
  'locktime': None
}

return { **txTemplate, **tx }

$$ language plpython3u volatile;

-- example query for deepest nesting level:
--
--  select ins[1].outpoint.hash
--  from btcd_bitcoin_raw_transaction('http://nobody:supersecret@some.mine:8332');

---   Block-related objects

drop type bitcoin_block cascade ;
create type bitcoin_block as (
	hash char(64),
	confirmations int,
	strippedsize int,
	size int,
	weight int,
	height int,
	version int,
	"versionHex" char(8),
	merkleroot char(64),
	tx char(64)[],
	time int,
	mediantime int,
	nonce bigint,
	bits char(8),
	difficulty numeric,
	chainwork char(64),
	"nTx" int,
	previousblockhash char(64),
	nextblockhash char(64)
) ;

create or replace function btcd_bitcoin_block(arg_url text, arg_block_hash text default '00000000000000000009c78d56ba39c076acf1233243beaa4f07daf221eb41a3')
returns bitcoin_block
as $$

from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException
from os import environ

url = None

if arg_url is not None:
    # Use a "http://user:password@host:port" URL string from the argument list
    url = arg_url

else:
    # Postgres must be started with the
    # following variables in the environment,
    # each prefixed by btcd_rpc_ (e.g. btcd_rpc_host)
    rpc_vars = ['host', 'port', 'user', 'password']

    required_vars = set('btcd_rpc_' + v for v in rpc_vars)
    defined_vars = set(environ).intersection(required_vars)
    missing_vars = required_vars.difference(defined_vars)

    if missing_vars and not arg_url:
        message = "The following " if len(defined_vars) else "No "
        message += "environment variables were detected..."
        plpy.notice(message)
        plpy.notice(defined_vars if len(defined_vars) else '')
        plpy.notice("")
        plpy.notice("Please set these environment variables in the postgres user environment and restart:")
        plpy.notice("    '" + "', '".join(missing_vars) + "'")
        plpy.notice("")
        plpy.error("Missing required btcd environment variables or URL argument")

    rpc_config = { k: environ.get('btcd_rpc_' + k) for k in rpc_vars }

    url = "http://{user}:{password}@{host}:{port}".format(**rpc_config)

try:
    rpc_connection = AuthServiceProxy(url)
    node_info = rpc_connection.getblockchaininfo()
    plpy.info("Current block height: {}".format(node_info.get('headers')))

except JSONRPCException as e:
    plpy.error(e.message)

return rpc_connection.getblock(arg_block_hash)

$$ language plpython3u volatile;

create or replace function btcd_bitcoin_blocknumber(arg_url text, arg_block_number bigint default 1)
returns bitcoin_block
as $$

from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException
from os import environ

url = None

if arg_url is not None:
    # Use a "http://user:password@host:port" URL string from the argument list
    url = arg_url

else:
    # Postgres must be started with the
    # following variables in the environment,
    # each prefixed by btcd_rpc_ (e.g. btcd_rpc_host)
    rpc_vars = ['host', 'port', 'user', 'password']

    required_vars = set('btcd_rpc_' + v for v in rpc_vars)
    defined_vars = set(environ).intersection(required_vars)
    missing_vars = required_vars.difference(defined_vars)

    if missing_vars and not arg_url:
        message = "The following " if len(defined_vars) else "No "
        message += "environment variables were detected..."
        plpy.notice(message)
        plpy.notice(defined_vars if len(defined_vars) else '')
        plpy.notice("")
        plpy.notice("Please set these environment variables in the postgres user environment and restart:")
        plpy.notice("    '" + "', '".join(missing_vars) + "'")
        plpy.notice("")
        plpy.error("Missing required btcd environment variables or URL argument")

    rpc_config = { k: environ.get('btcd_rpc_' + k) for k in rpc_vars }

    url = "http://{user}:{password}@{host}:{port}".format(**rpc_config)

try:
    rpc_connection = AuthServiceProxy(url)
    node_info = rpc_connection.getblockchaininfo()
    plpy.info("Current block height: {}".format(node_info.get('headers')))

except JSONRPCException as e:
    plpy.error(e.message)

return rpc_connection.getblock(rpc_connection.getblockhash(arg_block_number))

$$ language plpython3u volatile;

---   Metadata/network info related objects

drop type bitcoin_blockchain_info cascade;
drop type bitcoin_bip9_softforks;
drop type bitcoin_bip9_softfork_meta;
drop type bitcoin_softfork;
drop type bitcoin_softfork_status;

create type bitcoin_softfork_status as (
	status boolean
);

create type bitcoin_softfork as (
	id text,
	version int,
	reject bitcoin_softfork_status
);

create type bitcoin_bip9_softfork_meta as (
	status text,
	"startTime" int,
	timeout int,
	since int
);

create type bitcoin_bip9_softforks as (
	csv bitcoin_bip9_softfork_meta,
	segwit bitcoin_bip9_softfork_meta
);

create type bitcoin_blockchain_info as (
	chain text,
	blocks int,
	headers int,
	bestblockhash char(64),
	difficulty numeric,
	mediantime int,
	verificationprogress numeric,
	initialblockdownload boolean,
	chainwork char(64),
	size_on_disk bigint,
	pruned boolean,
	softforks bitcoin_softfork[],
	"bip9_softforks" bitcoin_bip9_softforks,
	warnings text
);

create or replace function btcd_bitcoin_network_info(arg_url text)
returns bitcoin_blockchain_info
as $$

from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException
from os import environ

url = None

if arg_url is not None:
    # Use a "http://user:password@host:port" URL string from the argument list
    url = arg_url

else:
    # Postgres must be started with the
    # following variables in the environment,
    # each prefixed by btcd_rpc_ (e.g. btcd_rpc_host)
    rpc_vars = ['host', 'port', 'user', 'password']

    required_vars = set('btcd_rpc_' + v for v in rpc_vars)
    defined_vars = set(environ).intersection(required_vars)
    missing_vars = required_vars.difference(defined_vars)

    if missing_vars and not arg_url:
        message = "The following " if len(defined_vars) else "No "
        message += "environment variables were detected..."
        plpy.notice(message)
        plpy.notice(defined_vars if len(defined_vars) else '')
        plpy.notice("")
        plpy.notice("Please set these environment variables in the postgres user environment and restart:")
        plpy.notice("    '" + "', '".join(missing_vars) + "'")
        plpy.notice("")
        plpy.error("Missing required btcd environment variables or URL argument")

    rpc_config = { k: environ.get('btcd_rpc_' + k) for k in rpc_vars }

    url = "http://{user}:{password}@{host}:{port}".format(**rpc_config)

try:
    rpc_connection = AuthServiceProxy(url)
    info = rpc_connection.getblockchaininfo()
    plpy.info("Current block height: {}".format(info.get('headers')))

except JSONRPCException as e:
    plpy.error(e.message)

return info

$$ language plpython3u volatile;


-- example query for deepest nesting level:
-- (note: Postgres arrays are indexed starting from 1)
--
--  select softforks[1].id 
--  from btcd_bitcoin_network_info('http://nobody:supersecret@some.mine:8332');
