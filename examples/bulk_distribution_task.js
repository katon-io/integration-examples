const fs = require("fs");
const readline = require("readline");
const stream = fs.createReadStream("./data/pioneers.csv");
const rl = readline.createInterface({ input: stream });

require("dotenv").config();

const katon = require("katon.io-js-sdk");
let data = [];

const PROJECT_ID = process.env.PROJECT_ID;
const CREDENTIALS = require(process.env.CREDENTIALS_PATH);
const COIN = process.env.COIN_ID;

const ENV = katon.KatonEnvironments.prod;

const _ = require("lodash");

var validator = require("email-validator");

rl.on("line", (row) => {
  const split = row.split(",");
  data.push({ email: split[1], project: split[4] });
});
rl.on("close", async () => {
  const pioneers = processData(data);
  // const pioneers = require('../failures/failed_accounts_creations.json')

  const ctx = katon.KatonIO.privateCtx(
    PROJECT_ID,
    CREDENTIALS.publicKey,
    CREDENTIALS.privateKey,
    { env: ENV }
  );

  const accounts = [];
  const failedAccountCreation = [];

  const size = 30;

  const chunks = _.chunk(pioneers, size);

  for (let index = 0; index < chunks.length; index++) {
    const res = await Promise.all(
      chunks[index].map(async (pioneer, i) => {
        console.log("Creating account for ", pioneer.email);
        const account = await ctx.accounts
          .createOrFetch(pioneer.email)
          .catch((error) => {
            failedAccountCreation.push(pioneer);
            console.log("❌ Failed: ", pioneer);
            console.log(error);
          });

        if (account) {
          accounts.push(account);
          console.log("✅ Success: ", pioneer);
        }
        console.log(index * size + i + 1 + "/" + pioneers.length, "Done");

        console.log("-----------------------");
      })
    );
  }

  // TO RERUN IF THERE ARE ANY FAILURES
  // const accountsOfFailedTransfers = require('../failures/failed_transfers_1688461804024.json')

  // accountsOfFailedTransfers.forEach((id) => {
  //     accounts.push(ctx.accounts.withUuid(id))
  // })

  console.log("Total Success", accounts.length);
  console.log("Total Failures", failedAccountCreation.length);

  fs.writeFileSync(
    `./failures/failed_accounts_creations_${Date.now()}.json`,
    JSON.stringify(failedAccountCreation)
  );

  const failedTransfers = [];

  const txChunks = _.chunk(accounts, size);

  // transfer
  for (let index = 0; index < txChunks.length; index++) {
    await Promise.all(
      txChunks[index].map(async (account, i) => {
        console.log(
          "Transferring for:",
          account.id,
          index + "/" + accounts.length
        );
        const res = await ctx
          .doPost(`/v1/transfers/coins/${COIN}`, {
            amount: "10000",
            toAccount: account.id,
          })
          .catch((error) => {
            failedTransfers.push(account.id);
            console.log("❌ Failed: ", account.id);
          });

        if (res) {
          console.log("✅ Success: ", account.id);
        }

        console.log(index * size + i + 1 + "/" + accounts.length, "Done");
        console.log("-----------------------");
      })
    );
  }

  console.log("Total Success", accounts.length - failedTransfers.length);
  console.log("Total Failures", failedTransfers.length);

  fs.writeFileSync(
    `./failures/failed_transfers_${Date.now()}.json`,
    JSON.stringify(failedTransfers)
  );
});

function processData(dt) {
  let filtered = dt.filter((row) => validator.validate(row.email));
  filtered = _.uniqBy(filtered, (row) => row.email);
  filtered = _.uniqBy(filtered, (row) => row.project);

  return filtered;
}
