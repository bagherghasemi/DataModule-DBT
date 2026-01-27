  - name: analytics_ad_accounts
    description: >
      A list of Meta ad accounts and their basic settings. This table provides structural
      information about accounts, not performance.
    columns:
      - name: ad_account_id
        description: Unique identifier for the ad account.
      - name: account_name
        description: Name of the ad account.
      - name: account_status
        description: Meta-reported status of the ad account.
      - name: currency
        description: Currency used for billing in this ad account.
      - name: timezone_name
        description: Timezone of the ad account.
      - name: created_time
        description: When the ad account was created.
  - name: analytics_campaigns
    description: >
      A list of Meta campaigns and how they are configured. This table describes what
      campaigns exist, not how they perform.
    columns:
      - name: campaign_id
        description: Unique identifier for the campaign.
      - name: ad_account_id
        description: Ad account this campaign belongs to.
      - name: campaign_name
        description: Name of the campaign.
      - name: objective
        description: Meta’s stated objective for the campaign.
      - name: status
        description: Meta-reported status of the campaign.
      - name: created_time
        description: When the campaign was created.
  - name: analytics_ad_sets
    description: >
      A list of Meta ad sets, which define targeting rules, budgets, and delivery settings.
      This table represents structure, not performance.
    columns:
      - name: ad_set_id
        description: Unique identifier for the ad set.
      - name: campaign_id
        description: Campaign this ad set belongs to.
      - name: ad_account_id
        description: Ad account this ad set belongs to.
      - name: ad_set_name
        description: Name of the ad set.
      - name: status
        description: Meta-reported status of the ad set.
      - name: daily_budget
        description: Daily budget for the ad set, if set.
      - name: created_time
        description: When the ad set was created.
  - name: analytics_ads
    description: >
      A list of individual Meta ads and their creative setup. This table represents
      what ads exist, not how they perform.
    columns:
      - name: ad_id
        description: Unique identifier for the ad.
      - name: ad_set_id
        description: Ad set this ad belongs to.
      - name: campaign_id
        description: Campaign this ad belongs to.
      - name: ad_account_id
        description: Ad account this ad belongs to.
      - name: ad_name
        description: Name of the ad.
      - name: status
        description: Meta-reported status of the ad.
      - name: creative_id
        description: Identifier of the creative used by the ad, if available.
      - name: created_time
        description: When the ad was created.
  - name: analytics_ad_performance_daily
    description: >
      The raw daily performance facts from Meta. Each row represents what happened
      for one ad on one specific day.
    columns:
      - name: ad_id
        description: Ad being measured.
      - name: ad_set_id
        description: Ad set the ad belongs to.
      - name: campaign_id
        description: Campaign the ad belongs to.
      - name: ad_account_id
        description: Ad account the ad belongs to.
      - name: date
        description: Date of the performance data.
      - name: impressions
        description: Number of times the ad was shown.
      - name: clicks
        description: Number of clicks on the ad.
      - name: spend
        description: Amount spent on the ad that day.
      - name: conversions
        description: Meta-reported conversions for the day, if available.
      - name: currency
        description: Currency of the reported spend.
