---Identify customers with multiple high-value withdrawals within a short time.

SELECT
cd.customer_id,
cd.full_name,
cd.email,
cd.phone_number,
COUNT(hvw.transaction_id) AS num_withdrawals,
MIN(hvw.transaction_date) AS first_withdrawal_time,
MAX(hvw.transaction_date) AS last_withdrawal_time
FROM (SELECT
customer_id,
transaction_id,
transaction_date,
amount,
LAG(transaction_date) OVER (PARTITION BY customer_id ORDER BY transaction_date) AS prev_transaction_date
FROM bank-transactions-analysis.Banktransactions.Bank_transactions
WHERE transaction_type = 'Withdrawal' AND amount > 4000
) as hvw
JOIN bank-transactions-analysis.Banktransactions.Customers_details as cd
ON hvw.customer_id = cd.customer_id
WHERE TIMESTAMP_DIFF(hvw.transaction_date, hvw.prev_transaction_date, HOUR) <= 48
GROUP BY
cd.customer_id,
cd.full_name,
cd.email,
cd.phone_number
HAVING COUNT(hvw.transaction_id) > 2
ORDER BY num_withdrawals DESC;


---- Detect frequent small transactions leading to a large withdrawal (smurfing behavior).


SELECT
cd.customer_id,
cd.full_name,
cd.email,
cd.phone_number,
COUNT(st.transaction_id) AS num_small_transactions,
SUM(st.amount) AS total_small_amount,
lw.transaction_id AS large_withdrawal_id,
lw.amount AS large_withdrawal_amount,
lw.transaction_date AS large_withdrawal_date
FROM (SELECT
customer_id,
transaction_id,
transaction_date,
amount
FROM bank-transactions-analysis.Banktransactions.Bank_transactions
WHERE transaction_type IN ('Deposit','Transfer') AND amount < 500 ) st
JOIN (SELECT
customer_id,
transaction_id,
transaction_date,
amount
FROM bank-transactions-analysis.Banktransactions.Bank_transactions
WHERE transaction_type = 'Withdrawal' AND amount > 2000 ) lw
ON st.customer_id = lw.customer_id
AND TIMESTAMP_DIFF(lw.transaction_date, st.transaction_date, HOUR) <= 24 
JOIN bank-transactions-analysis.Banktransactions.Customers_details cd
ON st.customer_id = cd.customer_id
GROUP BY
cd.customer_id,
cd.full_name,
cd.email,
cd.phone_number,
lw.transaction_id,
lw.amount,
lw.transaction_date
HAVING COUNT(st.transaction_id) >= 3
ORDER BY num_small_transactions DESC;



---- Find transactions with unusual locations or IP addresses that don’t match customer patterns.


SELECT
bt.transaction_id,
bt.customer_id,
cd.full_name,
cd.email,
cd.phone_number,
bt.transaction_date,
bt.location,
bt.ip_address,
bt.amount,
bt.transaction_type,
bt.merchant_name,
CASE
WHEN cth.location IS NULL THEN 'Unusual Location'
WHEN cth.ip_address IS NULL THEN 'Unusual IP Address'
ELSE 'Normal'
END AS flag
FROM bank-transactions-analysis.Banktransactions.Bank_transactions bt
LEFT JOIN (SELECT
customer_id,
location,
ip_address
FROM bank-transactions-analysis.Banktransactions.Bank_transactions
GROUP BY
customer_id,
location,
ip_address) cth
ON bt.customer_id = cth.customer_id
AND bt.location = cth.location
AND bt.ip_address = cth.ip_address
JOIN bank-transactions-analysis.Banktransactions.Customers_details cd
ON bt.customer_id = cd.customer_id
WHERE
cth.location IS NULL OR cth.ip_address IS NULL 
ORDER BY
bt.transaction_date DESC;


----Track card usage from multiple locations in an unreasonably short time (geographical inconsistency fraud)

WITH card_transactions AS (
SELECT
card_number,
transaction_id,
transaction_date,
location,
LAG(location) OVER (PARTITION BY card_number ORDER BY transaction_date) AS prev_location,
LAG(transaction_date) OVER (PARTITION BY card_number ORDER BY transaction_date) AS prev_transaction_date
FROM bank-transactions-analysis.Banktransactions.Bank_transactions
WHERE card_number IS NOT NULL ),
geographical_inconsistency AS (
SELECT
card_number,
transaction_id,
transaction_date,
location,
prev_location,
prev_transaction_date,
TIMESTAMP_DIFF(transaction_date, prev_transaction_date, MINUTE) AS time_diff_minutes
FROM card_transactions
WHERE prev_location IS NOT NULL AND location != prev_location AND TIMESTAMP_DIFF(transaction_date, prev_transaction_date, MINUTE) <= 60 )
SELECT
gi.card_number,
COUNT(gi.transaction_id) AS num_transactions,
MIN(gi.transaction_date) AS first_transaction_time,
MAX(gi.transaction_date) AS last_transaction_time,
ARRAY_AGG(gi.location) AS locations, 
ARRAY_AGG(gi.time_diff_minutes) AS time_diffs_minutes 
FROM geographical_inconsistency gi
GROUP BY
gi.card_number
HAVING COUNT(gi.transaction_id) > 1 
ORDER BY num_transactions DESC;


---2️. Transaction Patterns & Customer Insights
--- Determine top 3 transaction modes used by customers.

SELECT
transaction_mode,
COUNT(transaction_id) AS transaction_count
FROM bank-transactions-analysis.Banktransactions.Bank_transactions
GROUP BY transaction_mode
order by transaction_count desc
limit 3;


--- Find the average transaction amount per user and classify them into spending groups.


select
X.customer_id,
X.full_name,
X.email,
case 
when X.Transaction_amt between 0 and 1000 then "low_level"
when X.Transaction_amt between 1000 and 5000 then "mid_level"
else "high_level"
end as Spending_group,
X.Transaction_amt
from (select bt.customer_id,
cd.full_name,
cd.email,
round(avg(bt.amount),2) as Transaction_amt
from bank-transactions-analysis.Banktransactions.Bank_transactions as bt
join bank-transactions-analysis.Banktransactions.Customers_details as cd
on bt.customer_id=cd.customer_id
group by bt.customer_id,cd.full_name,cd.email)X


---  Identify customers who suddenly start making high-value transactions.

SELECT
cd.customer_id,
cd.full_name,
cd.email,
cd.phone_number,
hist.avg_transaction_amount AS historical_avg,
hist.max_transaction_amount AS historical_max,
recent.amount AS recent_high_value_amount,
recent.transaction_date AS recent_transaction_date
FROM (SELECT
customer_id,
AVG(amount) AS avg_transaction_amount,
MAX(amount) AS max_transaction_amount
FROM bank-transactions-analysis.Banktransactions.Bank_transactions
WHERE transaction_date < '2025-02-10'  
GROUP BY customer_id) hist
JOIN (SELECT
customer_id,
transaction_id,
transaction_date,
amount
FROM bank-transactions-analysis.Banktransactions.Bank_transactions
WHERE transaction_date >= '2025-02-10' 
AND amount > 5000) recent
ON hist.customer_id = recent.customer_id
JOIN bank-transactions-analysis.Banktransactions.Customers_details cd
ON recent.customer_id = cd.customer_id
WHERE recent.amount > 2 * hist.max_transaction_amount  
ORDER BY recent.amount DESC;


--- Analyze merchant categories to see where most transactions occur.

select merchant_category,
COUNT(transaction_id) AS transaction_count,
round(SUM(amount),2) AS total_transaction_amount,
round(AVG(amount),2) AS avg_transaction_amount
from bank-transactions-analysis.Banktransactions.Bank_transactions
WHERE merchant_category IS NOT NULL
GROUP BY merchant_category
ORDER BY transaction_count DESC;


--- Identify transactions exceeding regulatory thresholds (e.g., $9000).

SELECT
bt.transaction_id,
bt.customer_id,
cd.full_name,
cd.email,
bt.transaction_date,
bt.amount,
bt.transaction_type,
bt.merchant_name,
bt.location
FROM bank-transactions-analysis.Banktransactions.Bank_transactions bt
JOIN bank-transactions-analysis.Banktransactions.Customers_details cd
ON bt.customer_id = cd.customer_id
WHERE bt.amount > 9000 
ORDER BY bt.amount DESC;


--- Track international transactions and check if they comply with banking policies.

SELECT
bt.transaction_id,
bt.customer_id,
cd.full_name,
cd.email,
cd.phone_number,
bt.transaction_date,
bt.amount,
bt.transaction_type,
bt.merchant_name,
bt.location,
bt.is_foreign_transaction,
CASE
WHEN bt.amount > 5000 THEN 'Amount Exceeds Policy Limit'
WHEN bt.location IN ('High-Risk Country A', 'High-Risk Country B') THEN 'High-Risk Location'
ELSE 'Compliant'
END AS compliance_status
FROM bank-transactions-analysis.Banktransactions.Bank_transactions bt
JOIN bank-transactions-analysis.Banktransactions.Customers_details cd
ON bt.customer_id = cd.customer_id
WHERE bt.is_foreign_transaction = True
ORDER BY bt.transaction_date DESC;


--- Generate a suspicious transaction report for further investigation.

SELECT
st.transaction_id,
st.customer_id,
cd.full_name,
cd.email,
cd.phone_number,
st.transaction_date,
st.amount,
st.transaction_type,
st.location,
st.ip_address,
st.reason
FROM (
SELECT 
bt.transaction_id, 
bt.customer_id, 
bt.transaction_date, 
bt.amount, 
bt.transaction_type, 
bt.location, 
bt.ip_address, 
'High-Value Transaction' AS reason
FROM `bank-transactions-analysis.Banktransactions.Bank_transactions` bt
WHERE bt.amount > 9000
UNION ALL
SELECT 
bt.transaction_id, 
bt.customer_id, 
bt.transaction_date, 
bt.amount, 
bt.transaction_type, 
bt.location, 
bt.ip_address, 
'Unusual Location or IP Address' AS reason
FROM `bank-transactions-analysis.Banktransactions.Bank_transactions` bt
LEFT JOIN (SELECT 
customer_id, 
location, 
ip_address
FROM `bank-transactions-analysis.Banktransactions.Bank_transactions`
GROUP BY customer_id, location, ip_address) cth
ON bt.customer_id = cth.customer_id AND bt.location = cth.location AND bt.ip_address = cth.ip_address
WHERE cth.location IS NULL OR cth.ip_address IS NULL
UNION ALL
SELECT 
bt.transaction_id, 
bt.customer_id, 
bt.transaction_date, 
bt.amount, 
bt.transaction_type, 
bt.location, 
bt.ip_address, 
'Multiple Failed Transactions' AS reason
FROM `bank-transactions-analysis.Banktransactions.Bank_transactions` bt
WHERE bt.transaction_type = 'Failed Payment'
AND bt.customer_id IN (SELECT customer_id
FROM `bank-transactions-analysis.Banktransactions.Bank_transactions`
WHERE transaction_type = 'Failed Payment'
GROUP BY customer_id
HAVING COUNT(transaction_id) > 3)
UNION ALL
SELECT 
bt.transaction_id, 
bt.customer_id, 
bt.transaction_date, 
bt.amount, 
bt.transaction_type, 
bt.location, 
bt.ip_address, 
'Geographical Inconsistency' AS reason
FROM `bank-transactions-analysis.Banktransactions.Bank_transactions` bt
JOIN (SELECT card_number, 
location, 
transaction_date, 
LAG(location) OVER (PARTITION BY card_number ORDER BY transaction_date) AS prev_location, 
LAG(transaction_date) OVER (PARTITION BY card_number ORDER BY transaction_date) AS prev_transaction_date
FROM `bank-transactions-analysis.Banktransactions.Bank_transactions`
WHERE card_number IS NOT NULL) gi
ON bt.card_number = gi.card_number AND bt.transaction_date = gi.transaction_date
WHERE gi.prev_location IS NOT NULL AND gi.location != gi.prev_location AND TIMESTAMP_DIFF(bt.transaction_date, gi.prev_transaction_date, MINUTE) <= 60
UNION ALL
SELECT 
bt.transaction_id, 
bt.customer_id, 
bt.transaction_date, 
bt.amount, 
bt.transaction_type, 
bt.location, bt.ip_address, 
'Sudden High-Value Transaction' AS reason
FROM `bank-transactions-analysis.Banktransactions.Bank_transactions` bt
JOIN (SELECT customer_id, MAX(amount) AS max_transaction_amount
FROM `bank-transactions-analysis.Banktransactions.Bank_transactions`
WHERE transaction_date < '2025-02-10'
GROUP BY customer_id) hist
ON bt.customer_id = hist.customer_id
WHERE bt.transaction_date >= '2025-02-10' AND bt.amount > 2 * hist.max_transaction_amount
) st
JOIN `bank-transactions-analysis.Banktransactions.Customers_details` cd
ON st.customer_id = cd.customer_id
ORDER BY st.transaction_date DESC;
