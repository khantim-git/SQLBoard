SELECT MIN(work_date) AS first_working_day
FROM working_day
WHERE work_date >= DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM-01')
AND work_date < DATE_ADD(LAST_DAY(CURRENT_DATE()), 1)
