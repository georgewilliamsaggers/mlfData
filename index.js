import { query, runQuery } from "./database/hobbiton/index.js";




// Pick one, e.g.:
await query("SELECT * FROM integration_accounts limit 1");

// await query(sqlIntegrationClients);
// await query(sqlIntegrationTransactions);
// await query(sqlIntegrationLoans);

// Example without console logging:
// const result = await runQuery(sqlIntegrationLoans);
// console.log(result.rows);


// node index.js