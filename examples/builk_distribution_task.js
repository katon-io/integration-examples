const fs = require("fs");
const readline = require("readline");
const stream = fs.createReadStream("../data/pioneers.csv");
const rl = readline.createInterface({ input: stream });

const katon = require('katon.io-js-sdk')
let data = [];

const LOCAL_PROJECT_ID = 'bfb4550e-a34a-4c55-b280-f8686f04d3c0'
const SANDBOX_PROJECT_ID = 'd2066642-2642-4a03-a94f-f1bc64084252'

const LOCAL_CREDENTIALS = require('../credentials/katon_rewards_local.credentials.json')
const SANDBOX_CREDENTIALS = require('../credentials/milkyway_sandbox.credentials.json')

const LOCAL_COIN = 'REWARDS-c2e0ff'
const SANDBOX_COIN = 'MILKYWAY-20181d'

const PROJECT_ID = SANDBOX_PROJECT_ID
const CREDENTIALS = SANDBOX_CREDENTIALS
const COIN = SANDBOX_COIN

const ENV = katon.KatonEnvironments.sandbox

const _ = require('lodash');

var validator = require("email-validator");

 
rl.on("line", (row) => {
    const split = row.split(',')
    data.push({ email: split[1], project: split[4] });
});
 
rl.on("close", async () => {
    const pioneers = process(data);

    const ctx = katon.KatonIO.privateCtx(
        PROJECT_ID,
        CREDENTIALS.publicKey,
        CREDENTIALS.privateKey,
        { env:  ENV},
    );


    const accounts = [];
    const failedAccountCreation = [];

    const size = 30

    const chunks = _.chunk(pioneers, size)

    for (let index = 0; index < chunks.length; index++) {

        const res = await Promise.all(chunks[index].map(async (pioneer, i) => {
            console.log('Creating account for ', pioneer.email)
            const account = await ctx.accounts.createOrFetch(pioneer.email).catch((error) => {
                failedAccountCreation.push(pioneer);
                console.log('❌ Failed: ',  pioneer)
                console.log(error)
            })
    
            if (account) {
                accounts.push(account);
                            console.log('✅ Success: ',  pioneer)

            }
            console.log(index *  size + i + 1 +'/' + pioneers.length, 'Done')
    
            console.log('-----------------------')
        
        }))
    }

    // TO RERUN IF THERE ARE ANY FAILURES
    // const accountsOfFailedTransfers = require('../failures/failed_transfers.json')

    // accountsOfFailedTransfers.forEach((id) => {
    //     accounts.push(ctx.accounts.withUuid(id))
    // })


    console.log('Total Success', accounts.length)
    console.log('Total Failures', failedAccountCreation.length)

    fs.writeFileSync(`./credentials/failed_accounts_creations.json`, JSON.stringify(failedAccountCreation))


    const failedTransfers = [];

    const txChunks = _.chunk(accounts, size)


    // transfer
    for (let index = 0; index < txChunks.length; index++) {

        await Promise.all(txChunks[index].map(async (account, i) => {
            console.log('Transferring for:', account.id, index + '/' + accounts.length)
            const res = await ctx.doPost(`/v1/transfers/coins/${COIN}`, {
                amount: '10',
                toAccount: account.id
            }).catch((error) => {
                failedTransfers.push(account.id);
                console.log('❌ Failed: ',  account.id)
            })
    
            if (res) {
                console.log('✅ Success: ',  account.id)
            }
    
    
            console.log(index *  size + i + 1 + '/' + accounts.length, 'Done')
            console.log('-----------------------')
        }))
    }

    console.log('Total Success', accounts.length - failedTransfers.length)
    console.log('Total Failures', failedTransfers.length)

    fs.writeFileSync(`./credentials/failed_transfers.json`, JSON.stringify(failedTransfers))
});


function process(data) {
    data = data.filter((row) => validator.validate(row.email))
    data = _.uniqBy(data, (row) => row.email)
    data = _.uniqBy(data, (row) => row.project)

    return data
}