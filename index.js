import { query, runQuery } from "./database/hobbiton/index.js";

const sqlIntegrationAccounts = `SELECT * FROM partner_schema.integration_accounts LIMIT 1`;
const sqlIntegrationClients = `SELECT * FROM partner_schema.integration_clients LIMIT 20`;
const sqlIntegrationTransactions = `SELECT * FROM partner_schema.integration_transactions LIMIT 10`;
const sqlIntegrationLoans = `SELECT * FROM partner_schema.integration_loans LIMIT 1`;
const count = `SELECT 
    c.first_name,
    c.last_name,
    c.id,
    COUNT(t.id) AS transactions_made
FROM partner_schema.integration_clients c
JOIN partner_schema.integration_transactions t
    ON c.id = t.client_id
GROUP BY c.id, c.first_name, c.last_name
HAVING COUNT(t.id) > 30
ORDER BY transactions_made DESC;`;
const frequency = `-- Count of clients by number of transactions in last 3 months, starting from 0
SELECT 
    COALESCE(tx.tx_count, 0) AS transactions_made,
    COUNT(*) AS number_of_clients
FROM partner_schema.integration_clients c
LEFT JOIN (
    SELECT 
        client_id,
        COUNT(*) AS tx_count
    FROM partner_schema.integration_transactions
    WHERE date >= NOW() - INTERVAL '6 months'
    GROUP BY client_id
) tx
ON c.id = tx.client_id
GROUP BY COALESCE(tx.tx_count, 0)
ORDER BY transactions_made;`


// Pick one, e.g.:
await query(sqlIntegrationTransactions);

// await query(sqlIntegrationClients);
// await query(sqlIntegrationTransactions);
// await query(sqlIntegrationLoans);

// Example without console logging:
// const result = await runQuery(sqlIntegrationLoans);
// console.log(result.rows);
