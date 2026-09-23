-- QuickBooks Online in BigQuery: dataset punlabs.QBO.
--
-- Loaded daily by pipelines/qbo/load.py (Cloud Run job qbo-daily). One raw
-- table holds every record QuickBooks returns, one row per version; the views
-- flatten it. Query the views, not qbo_raw.
--
--   v_transactions        one row per current transaction (bills, expenses, deposits, ...)
--   v_transaction_lines   one row per line of those transactions
--   v_accounts / v_vendors / v_customers / v_items   lookup lists
--   v_transaction_changes edits and deletions seen since tracking began
--
-- Apply with:  bq query --use_legacy_sql=false < sql/qbo/ddl.sql

CREATE SCHEMA IF NOT EXISTS `punlabs.QBO`
OPTIONS (location = 'us-central1',
         description = 'QuickBooks Online (Pun Labs LLC, production company). Loaded daily at 06:00 ET by the qbo-daily Cloud Run job. Use the v_ views.');

CREATE TABLE IF NOT EXISTS `punlabs.QBO.qbo_raw` (
  entity          STRING    NOT NULL OPTIONS (description = 'QuickBooks entity type: Bill, Purchase, Deposit, JournalEntry, Account, Vendor, ...'),
  id              STRING    NOT NULL OPTIONS (description = 'QuickBooks Id, unique within entity'),
  sync_token      STRING    NOT NULL OPTIONS (description = 'QuickBooks version counter; increases on every edit'),
  txn_date        DATE               OPTIONS (description = 'Transaction date (NULL for list entities)'),
  created_at      TIMESTAMP          OPTIONS (description = 'When the record was created in QuickBooks'),
  last_updated_at TIMESTAMP          OPTIONS (description = 'When this version was saved in QuickBooks'),
  payload         JSON      NOT NULL OPTIONS (description = 'The record exactly as the QuickBooks API returned it'),
  first_seen_at   TIMESTAMP NOT NULL OPTIONS (description = 'First load that saw this version'),
  last_seen_at    TIMESTAMP NOT NULL OPTIONS (description = 'Most recent load that saw this version'),
  deleted_at      TIMESTAMP          OPTIONS (description = 'Set when the record stopped appearing in QuickBooks (deleted or voided-and-removed)'),
  run_id          STRING             OPTIONS (description = 'Load run that first inserted this version')
)
CLUSTER BY entity, id
OPTIONS (description = 'Every QuickBooks record, one row per version (entity, id, sync_token). Edits add rows; deletions set deleted_at. Use the v_ views.');

-- Current version of every record that still exists.
CREATE OR REPLACE VIEW `punlabs.QBO.v_qbo_current`
OPTIONS (description = 'Latest version of every QuickBooks record that still exists. Building block for the other views.')
AS
SELECT * EXCEPT (rn) FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY entity, id
                               ORDER BY SAFE_CAST(sync_token AS INT64) DESC, last_updated_at DESC) AS rn
  FROM `punlabs.QBO.qbo_raw`
)
WHERE rn = 1 AND deleted_at IS NULL;

CREATE OR REPLACE VIEW `punlabs.QBO.v_accounts` (
  account_id           OPTIONS (description = 'QuickBooks account Id; joins to account_id in v_transactions and v_transaction_lines'),
  account_number       OPTIONS (description = 'Chart-of-accounts number, e.g. 5025'),
  account_name         OPTIONS (description = 'Account name without the number, e.g. Manufacturing Supplies'),
  fully_qualified_name OPTIONS (description = 'Parent:Child name for sub-accounts'),
  account_type         OPTIONS (description = 'Expense, Cost of Goods Sold, Income, Bank, Credit Card, Accounts Payable, ...'),
  account_sub_type     OPTIONS (description = 'QuickBooks detail type, e.g. AdvertisingPromotional'),
  classification       OPTIONS (description = 'Asset, Liability, Equity, Revenue or Expense'),
  parent_account_id    OPTIONS (description = 'Parent account Id for sub-accounts'),
  current_balance      OPTIONS (description = 'Balance QuickBooks reports for the account now'),
  is_active            OPTIONS (description = 'False for inactive (hidden) accounts')
)
OPTIONS (description = 'Chart of accounts, including inactive accounts.')
AS
SELECT
  id,
  JSON_VALUE(payload, '$.AcctNum'),
  JSON_VALUE(payload, '$.Name'),
  JSON_VALUE(payload, '$.FullyQualifiedName'),
  JSON_VALUE(payload, '$.AccountType'),
  JSON_VALUE(payload, '$.AccountSubType'),
  JSON_VALUE(payload, '$.Classification'),
  JSON_VALUE(payload, '$.ParentRef.value'),
  SAFE_CAST(JSON_VALUE(payload, '$.CurrentBalance') AS NUMERIC),
  SAFE_CAST(JSON_VALUE(payload, '$.Active') AS BOOL)
FROM `punlabs.QBO.v_qbo_current`
WHERE entity = 'Account';

CREATE OR REPLACE VIEW `punlabs.QBO.v_vendors` (
  vendor_id      OPTIONS (description = 'QuickBooks vendor Id'),
  vendor_name    OPTIONS (description = 'Display name used on bills and expenses'),
  company_name   OPTIONS (description = 'Company name, if different'),
  email          OPTIONS (description = 'Primary email'),
  open_balance   OPTIONS (description = 'Amount currently owed to the vendor'),
  is_1099        OPTIONS (description = 'Tracked for 1099 reporting'),
  is_active      OPTIONS (description = 'False for inactive vendors'),
  created_at     OPTIONS (description = 'When the vendor was added')
)
OPTIONS (description = 'Vendors (suppliers, contractors, service providers), including inactive.')
AS
SELECT
  id,
  JSON_VALUE(payload, '$.DisplayName'),
  JSON_VALUE(payload, '$.CompanyName'),
  JSON_VALUE(payload, '$.PrimaryEmailAddr.Address'),
  SAFE_CAST(JSON_VALUE(payload, '$.Balance') AS NUMERIC),
  SAFE_CAST(JSON_VALUE(payload, '$.Vendor1099') AS BOOL),
  SAFE_CAST(JSON_VALUE(payload, '$.Active') AS BOOL),
  created_at
FROM `punlabs.QBO.v_qbo_current`
WHERE entity = 'Vendor';

CREATE OR REPLACE VIEW `punlabs.QBO.v_customers` (
  customer_id    OPTIONS (description = 'QuickBooks customer Id'),
  customer_name  OPTIONS (description = 'Display name; sales channels such as Amazon and Etsy are set up as customers'),
  company_name   OPTIONS (description = 'Company name, if different'),
  email          OPTIONS (description = 'Primary email'),
  open_balance   OPTIONS (description = 'Amount the customer currently owes'),
  is_active      OPTIONS (description = 'False for inactive customers'),
  created_at     OPTIONS (description = 'When the customer was added')
)
OPTIONS (description = 'Customers, including inactive. Marketplaces (Amazon, Etsy, PayPal) appear here as customers.')
AS
SELECT
  id,
  JSON_VALUE(payload, '$.DisplayName'),
  JSON_VALUE(payload, '$.CompanyName'),
  JSON_VALUE(payload, '$.PrimaryEmailAddr.Address'),
  SAFE_CAST(JSON_VALUE(payload, '$.Balance') AS NUMERIC),
  SAFE_CAST(JSON_VALUE(payload, '$.Active') AS BOOL),
  created_at
FROM `punlabs.QBO.v_qbo_current`
WHERE entity = 'Customer';

CREATE OR REPLACE VIEW `punlabs.QBO.v_items` (
  item_id             OPTIONS (description = 'QuickBooks product/service Id'),
  item_name           OPTIONS (description = 'Product or service name'),
  sku                 OPTIONS (description = 'SKU, if set'),
  item_type           OPTIONS (description = 'Inventory, NonInventory, Service, Group, Category'),
  unit_price          OPTIONS (description = 'Default sales price'),
  purchase_cost       OPTIONS (description = 'Default purchase cost'),
  income_account_id   OPTIONS (description = 'Account credited when the item is sold'),
  expense_account_id  OPTIONS (description = 'Account debited when the item is bought'),
  is_active           OPTIONS (description = 'False for inactive items')
)
OPTIONS (description = 'Products and services, including inactive.')
AS
SELECT
  id,
  JSON_VALUE(payload, '$.Name'),
  JSON_VALUE(payload, '$.Sku'),
  JSON_VALUE(payload, '$.Type'),
  SAFE_CAST(JSON_VALUE(payload, '$.UnitPrice') AS NUMERIC),
  SAFE_CAST(JSON_VALUE(payload, '$.PurchaseCost') AS NUMERIC),
  JSON_VALUE(payload, '$.IncomeAccountRef.value'),
  JSON_VALUE(payload, '$.ExpenseAccountRef.value'),
  SAFE_CAST(JSON_VALUE(payload, '$.Active') AS BOOL)
FROM `punlabs.QBO.v_qbo_current`
WHERE entity = 'Item';

-- Transaction headers. The "account" is the money side of the transaction:
-- the bank or card an expense was paid from, the account a deposit went to,
-- A/P for bills. The other side is on the lines.
CREATE OR REPLACE VIEW `punlabs.QBO.v_transactions` (
  txn_type            OPTIONS (description = 'Bill, BillPayment, Purchase (expense/check/card charge), Deposit, JournalEntry, Transfer, CreditCardPayment, VendorCredit, Invoice, Payment, SalesReceipt, CreditMemo, RefundReceipt, PurchaseOrder, Estimate'),
  txn_id              OPTIONS (description = 'QuickBooks Id; unique within txn_type. Join to v_transaction_lines on (txn_type, txn_id)'),
  txn_date            OPTIONS (description = 'Transaction date'),
  due_date            OPTIONS (description = 'Due date (bills, invoices)'),
  doc_number          OPTIONS (description = 'Reference number: vendor invoice number on bills, check number, invoice number'),
  payee_type          OPTIONS (description = 'Vendor, Customer or Employee'),
  payee_id            OPTIONS (description = 'Id of the vendor/customer/employee; joins to v_vendors.vendor_id or v_customers.customer_id'),
  payee_name          OPTIONS (description = 'Vendor, customer or employee name'),
  account_id          OPTIONS (description = 'Money-side account: bank/card paid from, deposit-to account, A/P for bills, from-account for transfers'),
  account_name        OPTIONS (description = 'Name of account_id'),
  account_type        OPTIONS (description = 'Type of account_id: Bank, Credit Card, Accounts Payable, ...'),
  to_account_id       OPTIONS (description = 'Destination account for Transfer and CreditCardPayment'),
  to_account_name     OPTIONS (description = 'Name of to_account_id'),
  payment_type        OPTIONS (description = 'Cash, Check or CreditCard (expenses and bill payments)'),
  total_amount        OPTIONS (description = 'Transaction total, positive, in USD'),
  open_balance        OPTIONS (description = 'Amount still unpaid (bills, invoices); 0 when paid'),
  memo                OPTIONS (description = 'Private note on the transaction'),
  location            OPTIONS (description = 'QuickBooks location/department, if used'),
  line_count          OPTIONS (description = 'Number of detail lines (excludes subtotal lines)'),
  created_at          OPTIONS (description = 'When the transaction was entered in QuickBooks'),
  last_updated_at     OPTIONS (description = 'When it was last edited in QuickBooks'),
  version_count       OPTIONS (description = 'Number of versions seen by the loader (1 = never edited since tracking began)')
)
OPTIONS (description = 'One row per current QuickBooks transaction. Money in: Deposit, SalesReceipt, Payment. Money out: Purchase, BillPayment. Owed: Bill. Adjustments: JournalEntry.')
AS
WITH t AS (
  SELECT c.*, v.version_count
  FROM `punlabs.QBO.v_qbo_current` c
  JOIN (SELECT entity, id, COUNT(*) AS version_count FROM `punlabs.QBO.qbo_raw` GROUP BY 1, 2) v USING (entity, id)
  WHERE c.entity IN ('Bill', 'BillPayment', 'Purchase', 'Deposit', 'JournalEntry', 'Transfer', 'CreditCardPayment',
                     'VendorCredit', 'PurchaseOrder', 'Invoice', 'Payment', 'SalesReceipt', 'CreditMemo',
                     'RefundReceipt', 'Estimate')
),
h AS (
  SELECT
    entity, id, txn_date, payload, created_at, last_updated_at, version_count,
    COALESCE(JSON_VALUE(payload, '$.AccountRef.value'), JSON_VALUE(payload, '$.DepositToAccountRef.value'),
             JSON_VALUE(payload, '$.CheckPayment.BankAccountRef.value'), JSON_VALUE(payload, '$.CreditCardPayment.CCAccountRef.value'),
             JSON_VALUE(payload, '$.FromAccountRef.value'), JSON_VALUE(payload, '$.BankAccountRef.value'),
             JSON_VALUE(payload, '$.APAccountRef.value'), JSON_VALUE(payload, '$.ARAccountRef.value')) AS account_id,
    COALESCE(JSON_VALUE(payload, '$.ToAccountRef.value'), JSON_VALUE(payload, '$.CreditCardAccountRef.value')) AS to_account_id,
    CASE
      WHEN JSON_VALUE(payload, '$.VendorRef.value') IS NOT NULL THEN 'Vendor'
      WHEN JSON_VALUE(payload, '$.CustomerRef.value') IS NOT NULL THEN 'Customer'
      ELSE JSON_VALUE(payload, '$.EntityRef.type')
    END AS payee_type,
    COALESCE(JSON_VALUE(payload, '$.VendorRef.value'), JSON_VALUE(payload, '$.CustomerRef.value'), JSON_VALUE(payload, '$.EntityRef.value')) AS payee_id,
    COALESCE(JSON_VALUE(payload, '$.VendorRef.name'), JSON_VALUE(payload, '$.CustomerRef.name'), JSON_VALUE(payload, '$.EntityRef.name')) AS payee_name
  FROM t
)
SELECT
  h.entity,
  h.id,
  h.txn_date,
  SAFE_CAST(JSON_VALUE(h.payload, '$.DueDate') AS DATE),
  JSON_VALUE(h.payload, '$.DocNumber'),
  h.payee_type,
  h.payee_id,
  h.payee_name,
  h.account_id,
  a.account_name,
  a.account_type,
  h.to_account_id,
  ta.account_name,
  COALESCE(JSON_VALUE(h.payload, '$.PaymentType'), JSON_VALUE(h.payload, '$.PayType')),
  COALESCE(SAFE_CAST(JSON_VALUE(h.payload, '$.TotalAmt') AS NUMERIC), SAFE_CAST(JSON_VALUE(h.payload, '$.Amount') AS NUMERIC)),
  SAFE_CAST(JSON_VALUE(h.payload, '$.Balance') AS NUMERIC),
  JSON_VALUE(h.payload, '$.PrivateNote'),
  JSON_VALUE(h.payload, '$.DepartmentRef.name'),
  (SELECT COUNT(*) FROM UNNEST(JSON_QUERY_ARRAY(h.payload, '$.Line')) l
    WHERE COALESCE(JSON_VALUE(l, '$.DetailType'), '') != 'SubTotalLineDetail'),
  h.created_at,
  h.last_updated_at,
  h.version_count
FROM h
LEFT JOIN `punlabs.QBO.v_accounts` a  ON a.account_id = h.account_id
LEFT JOIN `punlabs.QBO.v_accounts` ta ON ta.account_id = h.to_account_id;

-- Transaction lines. Account comes from the line itself or, for product lines,
-- from the product's income/expense account.
CREATE OR REPLACE VIEW `punlabs.QBO.v_transaction_lines` (
  txn_type           OPTIONS (description = 'Transaction type; see v_transactions'),
  txn_id             OPTIONS (description = 'Transaction Id; join to v_transactions on (txn_type, txn_id)'),
  txn_date           OPTIONS (description = 'Transaction date'),
  doc_number         OPTIONS (description = 'Transaction reference number'),
  payee_name         OPTIONS (description = 'Vendor/customer on the transaction header'),
  line_number        OPTIONS (description = 'Line position, 1-based'),
  line_type          OPTIONS (description = 'QuickBooks DetailType: AccountBasedExpenseLineDetail (category line), ItemBasedExpenseLineDetail, SalesItemLineDetail, JournalEntryLineDetail, DepositLineDetail, DiscountLineDetail; NULL for payment lines that only link to a bill or invoice'),
  description        OPTIONS (description = 'Line description, e.g. SKU, service period, hours worked'),
  amount             OPTIONS (description = 'Line amount as entered, positive'),
  posting_type       OPTIONS (description = 'Debit or Credit (journal entry lines only)'),
  signed_amount      OPTIONS (description = 'Journal entry lines: debit positive, credit negative. Other lines: same as amount'),
  account_id         OPTIONS (description = 'Account the line posts to (expense category, income account, journal account); joins to v_accounts'),
  account_number     OPTIONS (description = 'Chart-of-accounts number of account_id'),
  account_name       OPTIONS (description = 'Name of account_id'),
  account_type       OPTIONS (description = 'Type of account_id: Expense, Cost of Goods Sold, Income, ...'),
  item_id            OPTIONS (description = 'Product/service Id on product lines; joins to v_items'),
  item_name          OPTIONS (description = 'Product/service name'),
  quantity           OPTIONS (description = 'Quantity on product lines'),
  unit_price         OPTIONS (description = 'Unit price or cost on product lines'),
  line_payee_type    OPTIONS (description = 'Vendor/Customer/Employee named on the line (journal entry and deposit lines; billable customer on expense lines)'),
  line_payee_name    OPTIONS (description = 'Name of the line payee'),
  class_name         OPTIONS (description = 'QuickBooks class, if used'),
  billable_status    OPTIONS (description = 'Billable, NotBillable or HasBeenBilled'),
  linked_txn_type    OPTIONS (description = 'For payment and deposit lines: type of the transaction this line pays or deposits (Bill, Invoice, Payment, ...)'),
  linked_txn_id      OPTIONS (description = 'Id of that linked transaction')
)
OPTIONS (description = 'One row per line of every current QuickBooks transaction (subtotal lines excluded). Expense categories, products, journal debits/credits and which bill each payment paid.')
AS
WITH l AS (
  SELECT
    c.entity, c.id, c.txn_date, c.payload, line, pos,
    JSON_VALUE(line, '$.DetailType') AS dt,
    COALESCE(JSON_VALUE(line, '$.SalesItemLineDetail.ItemRef.value'), JSON_VALUE(line, '$.ItemBasedExpenseLineDetail.ItemRef.value')) AS item_id
  FROM `punlabs.QBO.v_qbo_current` c,
       UNNEST(JSON_QUERY_ARRAY(c.payload, '$.Line')) AS line WITH OFFSET pos
  WHERE c.entity IN ('Bill', 'BillPayment', 'Purchase', 'Deposit', 'JournalEntry', 'Transfer', 'CreditCardPayment',
                     'VendorCredit', 'PurchaseOrder', 'Invoice', 'Payment', 'SalesReceipt', 'CreditMemo',
                     'RefundReceipt', 'Estimate')
    AND COALESCE(JSON_VALUE(line, '$.DetailType'), '') != 'SubTotalLineDetail'
),
x AS (
  SELECT
    l.*,
    COALESCE(JSON_VALUE(line, '$.AccountBasedExpenseLineDetail.AccountRef.value'),
             JSON_VALUE(line, '$.JournalEntryLineDetail.AccountRef.value'),
             JSON_VALUE(line, '$.DepositLineDetail.AccountRef.value'),
             JSON_VALUE(line, '$.DiscountLineDetail.DiscountAccountRef.value'),
             IF(dt = 'SalesItemLineDetail', i.income_account_id, NULL),
             IF(dt = 'ItemBasedExpenseLineDetail', i.expense_account_id, NULL)) AS account_id,
    SAFE_CAST(JSON_VALUE(line, '$.Amount') AS NUMERIC) AS amount,
    JSON_VALUE(line, '$.JournalEntryLineDetail.PostingType') AS posting_type
  FROM l
  LEFT JOIN `punlabs.QBO.v_items` i ON i.item_id = l.item_id
)
SELECT
  x.entity,
  x.id,
  x.txn_date,
  JSON_VALUE(x.payload, '$.DocNumber'),
  COALESCE(JSON_VALUE(x.payload, '$.VendorRef.name'), JSON_VALUE(x.payload, '$.CustomerRef.name'), JSON_VALUE(x.payload, '$.EntityRef.name')),
  COALESCE(SAFE_CAST(JSON_VALUE(x.line, '$.LineNum') AS INT64), x.pos + 1),
  x.dt,
  JSON_VALUE(x.line, '$.Description'),
  x.amount,
  x.posting_type,
  IF(x.posting_type = 'Credit', -x.amount, x.amount),
  x.account_id,
  a.account_number,
  a.account_name,
  a.account_type,
  x.item_id,
  COALESCE(JSON_VALUE(x.line, '$.SalesItemLineDetail.ItemRef.name'), JSON_VALUE(x.line, '$.ItemBasedExpenseLineDetail.ItemRef.name')),
  COALESCE(SAFE_CAST(JSON_VALUE(x.line, '$.SalesItemLineDetail.Qty') AS NUMERIC), SAFE_CAST(JSON_VALUE(x.line, '$.ItemBasedExpenseLineDetail.Qty') AS NUMERIC)),
  COALESCE(SAFE_CAST(JSON_VALUE(x.line, '$.SalesItemLineDetail.UnitPrice') AS NUMERIC), SAFE_CAST(JSON_VALUE(x.line, '$.ItemBasedExpenseLineDetail.UnitPrice') AS NUMERIC)),
  COALESCE(JSON_VALUE(x.line, '$.JournalEntryLineDetail.Entity.Type'), JSON_VALUE(x.line, '$.DepositLineDetail.Entity.type'),
           IF(COALESCE(JSON_VALUE(x.line, '$.AccountBasedExpenseLineDetail.CustomerRef.value'),
                       JSON_VALUE(x.line, '$.ItemBasedExpenseLineDetail.CustomerRef.value')) IS NOT NULL, 'Customer', NULL)),
  COALESCE(JSON_VALUE(x.line, '$.JournalEntryLineDetail.Entity.EntityRef.name'), JSON_VALUE(x.line, '$.DepositLineDetail.Entity.name'),
           JSON_VALUE(x.line, '$.AccountBasedExpenseLineDetail.CustomerRef.name'), JSON_VALUE(x.line, '$.ItemBasedExpenseLineDetail.CustomerRef.name')),
  COALESCE(JSON_VALUE(x.line, '$.AccountBasedExpenseLineDetail.ClassRef.name'), JSON_VALUE(x.line, '$.ItemBasedExpenseLineDetail.ClassRef.name'),
           JSON_VALUE(x.line, '$.SalesItemLineDetail.ClassRef.name'), JSON_VALUE(x.line, '$.JournalEntryLineDetail.ClassRef.name'),
           JSON_VALUE(x.line, '$.DepositLineDetail.ClassRef.name')),
  COALESCE(JSON_VALUE(x.line, '$.AccountBasedExpenseLineDetail.BillableStatus'), JSON_VALUE(x.line, '$.ItemBasedExpenseLineDetail.BillableStatus')),
  JSON_VALUE(x.line, '$.LinkedTxn[0].TxnType'),
  JSON_VALUE(x.line, '$.LinkedTxn[0].TxnId')
FROM x
LEFT JOIN `punlabs.QBO.v_accounts` a ON a.account_id = x.account_id;

-- What changed, from the loader's point of view. History starts at the first
-- load: records that already existed then appear only if edited or deleted later.
CREATE OR REPLACE VIEW `punlabs.QBO.v_transaction_changes` (
  change_type        OPTIONS (description = 'created (new since tracking began), edited, or deleted'),
  changed_at         OPTIONS (description = 'QuickBooks save time for created/edited; the load that noticed it for deleted'),
  txn_type           OPTIONS (description = 'Transaction type'),
  txn_id             OPTIONS (description = 'Transaction Id'),
  txn_date           OPTIONS (description = 'Transaction date of this version'),
  doc_number         OPTIONS (description = 'Reference number of this version'),
  payee_name         OPTIONS (description = 'Vendor/customer of this version'),
  total_amount       OPTIONS (description = 'Total of this version'),
  previous_total     OPTIONS (description = 'Total of the prior version (edits only)'),
  sync_token         OPTIONS (description = 'QuickBooks version counter of this version')
)
OPTIONS (description = 'Transactions created, edited or deleted in QuickBooks since the QBO load began (2026-09-23). Use to see who is changing the books and when.')
AS
WITH v AS (
  SELECT
    r.*,
    COALESCE(SAFE_CAST(JSON_VALUE(payload, '$.TotalAmt') AS NUMERIC), SAFE_CAST(JSON_VALUE(payload, '$.Amount') AS NUMERIC)) AS total,
    LAG(COALESCE(SAFE_CAST(JSON_VALUE(payload, '$.TotalAmt') AS NUMERIC), SAFE_CAST(JSON_VALUE(payload, '$.Amount') AS NUMERIC)))
      OVER (PARTITION BY entity, id ORDER BY SAFE_CAST(sync_token AS INT64)) AS prev_total,
    ROW_NUMBER() OVER (PARTITION BY entity, id ORDER BY SAFE_CAST(sync_token AS INT64)) AS version_rank,
    ROW_NUMBER() OVER (PARTITION BY entity, id ORDER BY SAFE_CAST(sync_token AS INT64) DESC) AS latest_rank
  FROM `punlabs.QBO.qbo_raw` r
  WHERE entity NOT IN ('Account', 'Vendor', 'Customer', 'Item', 'Class', 'Department', 'Employee', 'Term', 'PaymentMethod')
),
tracking AS (SELECT MIN(first_seen_at) AS began FROM `punlabs.QBO.qbo_raw`),
events AS (
  SELECT IF(version_rank = 1, 'created', 'edited') AS change_type, last_updated_at AS changed_at, v.*
  FROM v, tracking
  WHERE v.first_seen_at > TIMESTAMP_ADD(tracking.began, INTERVAL 1 HOUR)
  UNION ALL
  SELECT 'deleted', deleted_at, v.*
  FROM v
  WHERE deleted_at IS NOT NULL AND latest_rank = 1
)
SELECT
  change_type, changed_at, entity, id, txn_date,
  JSON_VALUE(payload, '$.DocNumber'),
  COALESCE(JSON_VALUE(payload, '$.VendorRef.name'), JSON_VALUE(payload, '$.CustomerRef.name'), JSON_VALUE(payload, '$.EntityRef.name')),
  total,
  IF(change_type = 'edited', prev_total, NULL),
  sync_token
FROM events;
