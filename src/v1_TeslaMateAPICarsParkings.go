package main

import (
	"fmt"

	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
)

// TeslaMateAPICarsParkingsV1 func
func TeslaMateAPICarsParkingsV1(c *gin.Context) {

	// define error messages
	var CarsParkingsError1 = "Unable to load parkings."
	var CarsParkingsError2 = "Invalid date format."

	// getting CarID param from URL
	CarID := convertStringToInteger(c.Param("CarID"))
	// query options to modify query when collecting data
	ResultPage := convertStringToInteger(c.DefaultQuery("page", "1"))
	ResultShow := convertStringToInteger(c.DefaultQuery("show", "100"))

	// get startDate and endDate from query parameters
	parsedStartDate, err := parseDateParam(c.Query("startDate"))
	if err != nil {
		TeslaMateAPIHandleErrorResponse(c, "TeslaMateAPICarsParkingsV1", CarsParkingsError2, err.Error())
		return
	}
	parsedEndDate, err := parseDateParam(c.Query("endDate"))
	if err != nil {
		TeslaMateAPIHandleErrorResponse(c, "TeslaMateAPICarsParkingsV1", CarsParkingsError2, err.Error())
		return
	}

	// creating structs for /cars/<CarID>/parkings
	// Car struct - child of Data
	type Car struct {
		CarID   int        `json:"car_id"`   // smallint
		CarName NullString `json:"car_name"` // text (nullable)
	}
	// OdometerDetails struct - child of Parkings
	type OdometerDetails struct {
		OdometerStart    float64 `json:"odometer_start"`    // float64
		OdometerEnd      float64 `json:"odometer_end"`      // float64
		OdometerDistance float64 `json:"odometer_distance"` // float64
	}
	// BatteryDetails struct - child of Parkings
	type BatteryDetails struct {
		StartUsableBatteryLevel int  `json:"start_usable_battery_level"` // int
		StartBatteryLevel       int  `json:"start_battery_level"`        // int
		EndUsableBatteryLevel   int  `json:"end_usable_battery_level"`   // int
		EndBatteryLevel         int  `json:"end_battery_level"`          // int
		ReducedRange            bool `json:"reduced_range"`              // bool
		IsSufficientlyPrecise   bool `json:"is_sufficiently_precise"`    // bool
	}
	// PreferredRange struct - child of Parkings
	type PreferredRange struct {
		StartRange float64 `json:"start_range"` // float64
		EndRange   float64 `json:"end_range"`   // float64
		RangeDiff  float64 `json:"range_diff"`  // float64
	}
	// Parkings struct - child of Data
	type Parkings struct {
		StartDateTs      float64 `json:"start_date_ts"` // timestamp (ms)
		EndDateTs        float64 `json:"end_date_ts"`   // timestamp (ms)
		StartDate        string  `json:"start_date"`
		EndDate          string  `json:"end_date"`
		Duration         float64 `json:"duration"`
		Standby          float64 `json:"standby"`
		SocDiff          int64   `json:"soc_diff"`
		HasReducedRange  int     `json:"has_reduced_range"`
		RangeDiff        float64 `json:"range_diff"`
		Consumption      float64 `json:"consumption"`
		AvgPower         float64 `json:"avg_power"`
		RangeLostPerHour float64 `json:"range_lost_per_hour"`
		Latitude         float64 `json:"latitude"`  // float64
		Longitude        float64 `json:"longitude"` // float64
	}
	type TeslaMateUnits struct {
		UnitsLength      string `json:"unit_of_length"`      // string
		UnitsTemperature string `json:"unit_of_temperature"` // string
	}
	// Data struct - child of JSONData
	type Data struct {
		Car            Car            `json:"car"`
		Parkings       []Parkings     `json:"parkings"`
		TeslaMateUnits TeslaMateUnits `json:"units"`
	}
	// JSONData struct - main
	type JSONData struct {
		Data Data `json:"data"`
	}

	// creating required vars
	var (
		CarName                       NullString
		ParkingsData                  []Parkings
		UnitsLength, UnitsTemperature string
	)

	// calculate offset based on page (page 0 is not possible, since first page is minimum 1)
	if ResultPage > 0 {
		ResultPage--
	} else {
		ResultPage = 0
	}
	ResultPage = (ResultPage * ResultShow)

	// getting data from database
	query := `
		with merge as (
			SELECT
				c.start_date AS start_date,
				c.end_date AS end_date,
				c.start_ideal_range_km AS start_ideal_range_km,
				c.end_ideal_range_km AS end_ideal_range_km,
				c.start_rated_range_km AS start_rated_range_km,
				c.end_rated_range_km AS end_rated_range_km,
				start_battery_level,
				end_battery_level,
				p.usable_battery_level AS start_usable_battery_level,
				NULL AS end_usable_battery_level,
				p.odometer AS start_km,
				p.odometer AS end_km,
				p.latitude AS latitude,
				p.longitude AS longitude
			FROM charging_processes c
			JOIN positions p ON c.position_id = p.id
			WHERE c.car_id = $1
			UNION
			SELECT
				d.start_date AS start_date,
				d.end_date AS end_date,
				d.start_ideal_range_km AS start_ideal_range_km,
				d.end_ideal_range_km AS end_ideal_range_km,
				d.start_rated_range_km AS start_rated_range_km,
				d.end_rated_range_km AS end_rated_range_km,
				start_position.battery_level AS start_battery_level,
				end_position.battery_level AS end_battery_level,
				start_position.usable_battery_level AS start_usable_battery_level,
				end_position.usable_battery_level AS end_usable_battery_level,
				d.start_km AS start_km,
				d.end_km AS end_km,
				end_position.latitude AS latitude,
				end_position.longitude AS longitude
			FROM drives d
			JOIN positions start_position ON d.start_position_id = start_position.id
			JOIN positions end_position ON d.end_position_id = end_position.id
			WHERE d.car_id = $2
			), 
			v as (
			SELECT
				lag(t.end_date) OVER w AS start_date,
				t.start_date AS end_date,
				lag(t.end_rated_range_km) OVER w AS start_range,
				t.start_rated_range_km AS end_range,
				lag(t.end_km) OVER w AS start_km,
				t.start_km AS end_km,
				EXTRACT(EPOCH FROM age(t.start_date, lag(t.end_date) OVER w)) AS duration,
				lag(t.end_battery_level) OVER w AS start_battery_level,
				lag(t.end_usable_battery_level) OVER w AS start_usable_battery_level,
					start_battery_level AS end_battery_level,
					start_usable_battery_level AS end_usable_battery_level,
					start_battery_level > COALESCE(start_usable_battery_level, start_battery_level) AS has_reduced_range,
				lag(t.latitude) OVER w AS latitude,
				lag(t.longitude) OVER w AS longitude
			FROM merge t
			WINDOW w AS (ORDER BY t.start_date ASC)
			ORDER BY start_date DESC
			)

			SELECT
			c.name,
			(SELECT unit_of_length FROM settings LIMIT 1) as unit_of_length,
  			(SELECT unit_of_temperature FROM settings LIMIT 1) as unit_of_temperature,
			round(extract(epoch FROM v.start_date)) * 1000 AS start_date_ts,
			round(extract(epoch FROM v.end_date)) * 1000 AS end_date_ts,
			-- Columns
			v.start_date,
			v.end_date,
			v.duration,
			(coalesce(s_asleep.sleep, 0) + coalesce(s_offline.sleep, 0)) / v.duration as standby,
				-greatest(v.start_battery_level - v.end_battery_level, 0) as soc_diff,
				CASE WHEN has_reduced_range THEN 1 ELSE 0 END as has_reduced_range,
				convert_km(CASE WHEN has_reduced_range THEN 0 ELSE (v.start_range - v.end_range)::numeric END, 'km') AS range_diff,
			COALESCE(CASE WHEN has_reduced_range THEN 0 ELSE (v.start_range - v.end_range) * c.efficiency END, 0) AS consumption,
			COALESCE(CASE WHEN has_reduced_range THEN 0 ELSE ((v.start_range - v.end_range) * c.efficiency) / (v.duration / 3600) * 1000 END, 0) as avg_power,
			convert_km(CASE WHEN has_reduced_range THEN 0 ELSE ((v.start_range - v.end_range) / (v.duration / 3600))::numeric END, 'km') AS range_lost_per_hour,
			v.latitude,
			v.longitude
			FROM v,
			LATERAL (
				SELECT EXTRACT(EPOCH FROM sum(age(s.end_date, s.start_date))) as sleep
				FROM states s
				WHERE
				state = 'asleep' AND
				v.start_date <= s.start_date AND s.end_date <= v.end_date AND
				s.car_id = $3
			) s_asleep,
			LATERAL (
				SELECT EXTRACT(EPOCH FROM sum(age(s.end_date, s.start_date))) as sleep
				FROM states s
				WHERE
				state = 'offline' AND
				v.start_date <= s.start_date AND s.end_date <= v.end_date AND
				s.car_id = $4
			) s_offline
			JOIN cars c ON c.id = $5
			WHERE
			v.duration > (1 * 60 * 60)
			AND v.start_range - v.end_range >= 0
			AND v.end_km - v.start_km < 1`

	// Parameters to be passed to the query
	var queryParams []interface{}
	queryParams = append(queryParams, CarID, CarID, CarID, CarID, CarID)
	paramIndex := 6

	// Add date filtering if provided
	if parsedStartDate != "" {
		query += fmt.Sprintf(" AND v.start_date >= $%d", paramIndex)
		queryParams = append(queryParams, parsedStartDate)
		paramIndex++
	}
	if parsedEndDate != "" {
		query += fmt.Sprintf(" AND v.end_date <= $%d", paramIndex)
		queryParams = append(queryParams, parsedEndDate)
		paramIndex++
	}

	query += fmt.Sprintf(`
	  LIMIT $%d OFFSET $%d;`, paramIndex, paramIndex+1)

	queryParams = append(queryParams, ResultShow, ResultPage)

	rows, err := db.Query(query, queryParams...)

	// checking for errors in query
	if err != nil {
		TeslaMateAPIHandleErrorResponse(c, "TeslaMateAPICarsParkingsV1", CarsParkingsError1, err.Error())
		return
	}

	// defer closing rows
	defer rows.Close()

	// looping through all results
	for rows.Next() {

		// creating parking object based on struct
		parking := Parkings{}

		// scanning row and putting values into the parking
		err = rows.Scan(
			&CarName,
			&UnitsLength,
			&UnitsTemperature,
			&parking.StartDateTs,
			&parking.EndDateTs,
			&parking.StartDate,
			&parking.EndDate,
			&parking.Duration,
			&parking.Standby,
			&parking.SocDiff,
			&parking.HasReducedRange,
			&parking.RangeDiff,
			&parking.Consumption,
			&parking.AvgPower,
			&parking.RangeLostPerHour,
			&parking.Latitude,
			&parking.Longitude,
		)

		// converting values based of settings UnitsLength
		if UnitsLength == "mi" {
			parking.RangeDiff = kilometersToMiles(parking.RangeDiff)
			parking.RangeLostPerHour = kilometersToMiles(parking.RangeLostPerHour)
		}
		// converting values based of settings UnitsTemperature
		/*if UnitsTemperature == "F" {
			parking.OutsideTempAvg = celsiusToFahrenheit(parking.OutsideTempAvg)
			parking.InsideTempAvg = celsiusToFahrenheit(parking.InsideTempAvg)
		}*/

		// adjusting to timezone differences from UTC to be userspecific
		parking.StartDate = getTimeInTimeZone(parking.StartDate)
		parking.EndDate = getTimeInTimeZone(parking.EndDate)

		// checking for errors after scanning
		if err != nil {
			TeslaMateAPIHandleErrorResponse(c, "TeslaMateAPICarsParkingsV1", CarsParkingsError1, err.Error())
			return
		}

		// appending parking to ParkingsData
		ParkingsData = append(ParkingsData, parking)
	}

	// checking for errors in the rows result
	err = rows.Err()
	if err != nil {
		TeslaMateAPIHandleErrorResponse(c, "TeslaMateAPICarsParkingsV1", CarsParkingsError1, err.Error())
		return
	}

	// build the data-blob
	jsonData := JSONData{
		Data{
			Car: Car{
				CarID:   CarID,
				CarName: CarName,
			},
			Parkings: ParkingsData,
			TeslaMateUnits: TeslaMateUnits{
				UnitsLength:      UnitsLength,
				UnitsTemperature: UnitsTemperature,
			},
		},
	}

	// return jsonData
	TeslaMateAPIHandleSuccessResponse(c, "TeslaMateAPICarsParkingsV1", jsonData)
}
