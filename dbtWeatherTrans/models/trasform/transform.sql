WITH transformed_weather AS (
	SELECT
		CONVERT(DATE,Date_time) AS date,
		Name,
		AVG(tempreture) AS avg_temp,
		MAX(tempreture) AS max_temp,
		MIN(tempreture) AS min_temp,
		AVG(humidity) AS avg_humidity,
        AVG(pressure) AS avg_pressure,
		AVG(wind_speed) AS avg_wind_speed,
		AVG(wind_degree) AS avg_wind_degree
	FROM WeatherData
	GROUP BY 
		CONVERT(DATE,Date_time), Name
)
SELECT *, GETDATE() AS load_datetime
FROM transformed_weather