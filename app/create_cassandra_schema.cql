CREATE KEYSPACE IF NOT EXISTS op_risk
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS op_risk.risk_signals (
  ticker text,
  date text,
  accounting double,
  misstatement double,
  events double,
  risk double,
  risk_ind_adjs double,
  risk_level text,
  PRIMARY KEY (ticker, date)
);
