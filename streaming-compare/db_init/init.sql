CREATE TABLE IF NOT EXISTS mappings (
  ad_id TEXT PRIMARY KEY,
  campaign_id TEXT
);

INSERT INTO mappings (ad_id, campaign_id) VALUES
  ('ad_1', 'campaign_1'),
  ('ad_2', 'campaign_1'),
  ('ad_3', 'campaign_2'),
  ('ad_4', 'campaign_2')
ON CONFLICT DO NOTHING;
