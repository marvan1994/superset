description_en: "Full conversion funnel from impressions to completed orders, by ad platform."
description_ru: "Полная воронка конверсии от показов до завершённых заказов, с разбивкой по рекламным платформам."
sql: |
WITH CampaignAcquisitions AS (
SELECT
ac.campaign_id,
u.user_id
FROM ad_clicks ac
JOIN users u ON ac.user_temp_id = u.user_temp_id
),
AcquiredUsersWithOrders AS (
SELECT DISTINCT
ca.campaign_id,
uo.user_id
FROM CampaignAcquisitions ca
JOIN user_orders uo ON ca.user_id = uo.user_id
WHERE uo.status = 'completed'
)
SELECT
acamp.platform,
SUM(acamp.impressions) AS total_impressions,
SUM(acamp.clicks) AS total_clicks,
COUNT(DISTINCT ca.user_id) AS total_registrations,
COUNT(DISTINCT auwo.user_id) AS total_purchasing_users
FROM ad_campaigns acamp
LEFT JOIN CampaignAcquisitions ca ON acamp.campaign_id = ca.campaign_id
LEFT JOIN AcquiredUsersWithOrders auwo ON acamp.campaign_id = auwo.campaign_id
GROUP BY acamp.platform
ORDER BY total_impressions DESC;
description_en: "Calculate Cost Per Acquisition (CPA) for each campaign."
description_ru: "Расчет стоимости привлечения одного пользователя (CPA) для каждой рекламной кампании."
sql: |
WITH AcquiredUsers AS (
SELECT
ac.campaign_id,
COUNT(DISTINCT u.user_id) as user_count
FROM ad_clicks ac
JOIN users u ON ac.user_temp_id = u.user_temp_id
GROUP BY ac.campaign_id
)
SELECT
c.campaign_name,
c.platform,
c.spend_usd,
COALESCE(au.user_count, 0) AS users_acquired,
c.spend_usd / NULLIF(COALESCE(au.user_count, 0), 0) AS cost_per_acquisition
FROM ad_campaigns c
LEFT JOIN AcquiredUsers au ON c.campaign_id = au.campaign_id
ORDER BY cost_per_acquisition ASC;
description_en: "Daily Active Users (DAU) based on any activity."
description_ru: "Количество уникальных активных пользователей в день (DAU) на основе любой активности."
sql: |
SELECT
CAST(activity_timestamp AS DATE) AS activity_date,
COUNT(DISTINCT user_id) AS daily_active_users
FROM user_activity
GROUP BY activity_date
ORDER BY activity_date DESC
LIMIT 30;
description_en: "Top 10 most traded stocks by total dollar value."
description_ru: "Топ-10 самых торгуемых акций по общему объему в долларах."
sql: |
SELECT
s.stock_name,
s.ticker,
SUM(oi.quantity * oi.price_per_stock) AS total_value_traded
FROM order_items oi
JOIN stocks s ON oi.stock_id = s.stock_id
JOIN user_orders uo ON oi.order_id = uo.order_id
WHERE uo.status = 'completed'
GROUP BY s.stock_name, s.ticker
ORDER BY total_value_traded DESC
LIMIT 10;
description_en: "Monthly recurring revenue (MRR) from completed orders."
description_ru: "Ежемесячный регулярный доход (MRR) от завершённых заказов."
sql: |
SELECT
DATE_TRUNC('month', order_timestamp)::DATE AS month,
SUM(total_amount_usd) AS monthly_revenue
FROM user_orders
WHERE status = 'completed'
GROUP BY month
ORDER BY month;
description_en: "User Lifetime Value (LTV) by acquisition ad platform."
description_ru: "Пожизненная ценность клиента (LTV) в разрезе рекламной платформы, с которой он пришел."
sql: |
WITH UserAcquisitionPath AS (
SELECT DISTINCT ON (u.user_id)
u.user_id,
c.platform
FROM users u
JOIN ad_clicks ac ON u.user_temp_id = ac.user_temp_id
JOIN ad_campaigns c ON ac.campaign_id = c.campaign_id
ORDER BY u.user_id, ac.click_timestamp
)
SELECT
uap.platform,
COUNT(DISTINCT uap.user_id) AS total_users,
SUM(COALESCE(uo.total_amount_usd, 0)) AS total_revenue,
SUM(COALESCE(uo.total_amount_usd, 0)) / COUNT(DISTINCT uap.user_id) AS average_ltv
FROM UserAcquisitionPath uap
LEFT JOIN user_orders uo ON uap.user_id = uo.user_id AND uo.status = 'completed'
GROUP BY uap.platform
ORDER BY average_ltv DESC;
description_en: "Revenue by stock sector from completed orders."
description_ru: "Выручка по секторам акций от завершённых заказов."
sql: |
SELECT
s.sector,
SUM(oi.quantity * oi.price_per_stock) AS total_revenue
FROM order_items oi
JOIN stocks s ON oi.stock_id = s.stock_id
JOIN user_orders uo ON oi.order_id = uo.order_id
WHERE uo.status = 'completed'
GROUP BY s.sector
ORDER BY total_revenue DESC;
description_en: "Average time from first ad click to user registration."
description_ru: "Среднее время от первого клика по рекламе до регистрации пользователя."
sql: |
WITH FirstClick AS (
SELECT
user_temp_id,
MIN(click_timestamp) as first_click_time
FROM ad_clicks
GROUP BY user_temp_id
)
SELECT
AVG(u.registered_at - fc.first_click_time) AS avg_time_to_register
FROM users u
JOIN FirstClick fc ON u.user_temp_id = fc.user_temp_id;
description_en: "Monthly cohort analysis showing the number of users making their first purchase."
description_ru: "Ежемесячный когортный анализ, показывающий количество пользователей, совершивших первую покупку."
sql: |
WITH UserFirstOrder AS (
SELECT
user_id,
MIN(order_timestamp) as first_order_date
FROM user_orders
WHERE status = 'completed'
GROUP BY user_id
),
UserCohorts AS (
SELECT
u.user_id,
DATE_TRUNC('month', u.registered_at)::DATE AS registration_month,
DATE_TRUNC('month', ufo.first_order_date)::DATE AS first_order_month
FROM users u
LEFT JOIN UserFirstOrder ufo ON u.user_id = ufo.user_id
)
SELECT
registration_month,
(DATE_PART('year', first_order_month) - DATE_PART('year', registration_month)) * 12 +
(DATE_PART('month', first_order_month) - DATE_PART('month', registration_month)) AS months_since_registration,
COUNT(DISTINCT user_id) AS purchasing_users
FROM UserCohorts
WHERE first_order_month IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2;
description_en: "Most frequent user activities after registration."
description_ru: "Самые частые действия пользователей после регистрации."
sql: |
SELECT
activity_type,
COUNT(activity_id) AS event_count
FROM user_activity
GROUP BY activity_type
ORDER BY event_count DESC;
description_en: "Conversion rate from registration to first purchase, by signup platform."
description_ru: "Коэффициент конверсии из регистрации в первую покупку по платформе регистрации."
sql: |
WITH UserFirstOrder AS (
SELECT DISTINCT user_id FROM user_orders WHERE status = 'completed'
)
SELECT
u.signup_platform,
COUNT(u.user_id) AS total_users,
COUNT(ufo.user_id) AS purchasing_users,
(CAST(COUNT(ufo.user_id) AS REAL) / COUNT(u.user_id)) * 100 AS conversion_rate_percent
FROM users u
LEFT JOIN UserFirstOrder ufo ON u.user_id = ufo.user_id
GROUP BY u.signup_platform
ORDER BY conversion_rate_percent DESC;
description_en: "Order status distribution (completed, pending, cancelled)."
description_ru: "Распределение заказов по статусам (завершен, в ожидании, отменен)."
sql: |
SELECT
status,
COUNT(order_id) AS number_of_orders,
(COUNT(order_id) * 100.0 / SUM(COUNT(order_id)) OVER ()) AS percentage
FROM user_orders
GROUP BY status
ORDER BY number_of_orders DESC;
description_en: "Campaign performance breakdown by country and device."
description_ru: "Эффективность кампаний в разрезе стран и устройств."
sql: |
SELECT
c.campaign_name,
ac.country,
ac.device_type,
COUNT(ac.click_id) AS clicks,
COUNT(DISTINCT u.user_id) AS acquired_users
FROM ad_campaigns c
JOIN ad_clicks ac ON c.campaign_id = ac.campaign_id
LEFT JOIN users u ON ac.user_temp_id = u.user_temp_id
GROUP BY c.campaign_name, ac.country, ac.device_type
ORDER BY c.campaign_name, clicks DESC;
description_en: "Compare total investment value from referred vs. non-referred users."
description_ru: "Сравнение общего объёма инвестиций от пользователей, пришедших по реферальной программе и без неё."
sql: |
SELECT
CASE
WHEN u.referred_by IS NULL THEN 'Not Referred'
ELSE 'Referred'
END AS referral_status,
COUNT(DISTINCT u.user_id) AS number_of_users,
SUM(COALESCE(uo.total_amount_usd, 0)) AS total_investment,
SUM(COALESCE(uo.total_amount_usd, 0)) / COUNT(DISTINCT u.user_id) AS average_investment_per_user
FROM users u
LEFT JOIN user_orders uo ON u.user_id = uo.user_id AND uo.status = 'completed'
GROUP BY referral_status
ORDER BY total_investment DESC;