import { query, runQuery } from "./database/hobbiton/index.js";




// Pick one, e.g.:
await query(sqlIntegrationTransactions);

// await query(sqlIntegrationClients);
// await query(sqlIntegrationTransactions);
// await query(sqlIntegrationLoans);

// Example without console logging:
// const result = await runQuery(sqlIntegrationLoans);
// console.log(result.rows);
