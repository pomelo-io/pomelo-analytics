import fs from 'fs';
import { Substreams, download } from 'substreams';
const data = JSON.parse(fs.readFileSync('./data.json', "utf-8"));

// fixed parameters
const spkg = "https://github.com/pinax-network/substreams/releases/download/atomicmarket-v0.1.0/atomicmarket-v0.1.0.spkg";
const outputModule = "prom_out";
const host = "eos.firehose.eosnation.io:9001"

async function get_head_block_num() {
    const response = await fetch('https://eos.greymass.com/v1/chain/get_info');
    const json = await response.json();
    return json.head_block_num;
}

(async () => {
    // sink parameters
    const head_block_num = await get_head_block_num();
    const startBlockNum = String(data?.clock?.number ?? head_block_num);
    const stopBlockNum = data?.clock?.number ? `+${head_block_num - Number(data?.clock?.number)}` : `+1`;

    // Initialize Substreams
    const substreams = new Substreams(outputModule, {
        host,
        startBlockNum,
        stopBlockNum,
        productionMode: true,
        authorization: process.env.STREAMINGFAST_KEY // or SUBSTREAMS_API_TOKEN
    });

    // download Substream from IPFS
    const {modules, registry} = await download(spkg);

    // Find Protobuf message types from registry
    const PrometheusOperations = registry.findMessage("pinax.substreams.sink.prometheus.v1.PrometheusOperations");
    if ( !PrometheusOperations) throw new Error("Could not find PrometheusOperations message type");
    
    substreams.on("block", block => {
        data.clock = block.clock;
        data.last_cursor = block.cursor;
    });

    substreams.on("mapOutput", output => {
        const decoded: any = PrometheusOperations.fromBinary(output.data.mapOutput.value);
        for ( const { metric, operation, name, value, labels } of decoded.toJson().operations ) {
            if ( labels.collection_name != "pomelo" ) continue;
            console.log({ metric, operation, name, value, labels })
            if ( operation == "OPERATIONS_ADD" ) {
                const timestamp = data.clock.timestamp.seconds;
                data.atomicmarket.total_volume += value;
                data.atomicmarket.last_newsales.push([timestamp, value]);
            }
        }
    });

    // start streaming Substream
    await substreams.start(modules);

    // save output when finished
    fs.writeFileSync('./data.json', JSON.stringify(data, null, 2));
})();