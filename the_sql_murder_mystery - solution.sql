-- Crime scene report
SELECT description
FROM crime_scene_report
WHERE type = 'murder'
AND city = 'SQL City'
AND date = '20180115';

-- Interview with first witness
SELECT transcript
FROM interview i
JOIN person p ON i.person_id = p.id
WHERE p.address_street_name = 'Northwestern Dr'
ORDER BY address_number DESC
LIMIT 1;

-- Interview with second witness
SELECT transcript
FROM interview i
JOIN person p ON i.person_id = p.id
WHERE address_street_name = 'Franklin Ave'
AND name LIKE '%Annabel%';

-- Murderer
SELECT p.name
FROM person p
JOIN drivers_license d ON p.license_id = d.id
JOIN get_fit_now_member m ON p.id = m.person_id
JOIN get_fit_now_check_in c ON m.id = c.membership_id
WHERE c.check_in_date = '20180109'
AND m.id LIKE '48Z%'
AND m.membership_status = 'gold'
AND d.plate_number LIKE '%H42W%';

-- Interview with murderer
SELECT i.transcript
FROM interview i
JOIN person p ON i.person_id = p.id
JOIN drivers_license d ON p.license_id = d.id
JOIN get_fit_now_member m ON p.id = m.person_id
JOIN get_fit_now_check_in c ON m.id = c.membership_id
WHERE c.check_in_date = '20180109'
AND m.id LIKE "48Z%"
AND m.membership_status = 'gold'
AND d.plate_number LIKE '%H42W%';

-- Woman who hired the murderer
SELECT p.name, i.annual_income
FROM person p
JOIN income i ON p.ssn = i.ssn
JOIN drivers_license d ON p.license_id = d.id
JOIN facebook_event_checkin f ON p.id = f.person_id
WHERE gender = 'female'
AND height >= 65
AND height <= 67
AND hair_color = 'red'
AND car_make = 'Tesla'
AND car_model = 'Model S'
AND f.event_name = 'SQL Symphony Concert'
AND f.date >= '20171201'
AND f.date <= '20171231'
GROUP BY p.id
HAVING COUNT(f.event_id) >= 3;