package main

import (
	"fmt"

	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
)

// TeslaMateAPICarsPositionsV1 func
func TeslaMateAPICarsPositionsV1(c *gin.Context) {

	// define error messages
	var CarsPositionsError1 = "Unable to load positions."
	var CarsPositionsError2 = "Invalid date format."

	// getting CarID param from URL
	CarID := convertStringToInteger(c.Param("CarID"))
	// query options to modify query when collecting data
	ResultPage := convertStringToInteger(c.DefaultQuery("page", "1"))
	ResultShow := convertStringToInteger(c.DefaultQuery("show", "100"))

	// get startDate and endDate from query parameters
	parsedStartDate, err := parseDateParam(c.Query("startDate"))
	if err != nil {
		TeslaMateAPIHandleErrorResponse(c, "TeslaMateAPICarsPositionsV1", CarsPositionsError2, err.Error())
		return
	}
	parsedEndDate, err := parseDateParam(c.Query("endDate"))
	if err != nil {
		TeslaMateAPIHandleErrorResponse(c, "TeslaMateAPICarsPositionsV1", CarsPositionsError2, err.Error())
		return
	}

	// creating structs for /cars/<CarID>/positions
	// Car struct - child of Data
	type Car struct {
		CarID   int        `json:"car_id"`   // smallint
		CarName NullString `json:"car_name"` // text (nullable)
	}
	// BatteryDetails struct - child of Positions
	type BatteryDetails struct {
		BatteryLevel         int     `json:"battery_level"`          // int
		UsableBatteryLevel   int     `json:"usable_battery_level"`   // int
		BatteryHeater        bool    `json:"battery_heater"`         // bool
		BatteryHeaterOn      bool    `json:"battery_heater_on"`      // bool
		ChargeEnergyAdded    float64 `json:"charge_energy_added"`    // float64
		ChargerPower         int     `json:"charger_power"`          // int
		ChargerVoltage       int     `json:"charger_voltage"`        // int
		ChargerPhases        int     `json:"charger_phases"`         // int
		ChargerActualCurrent int     `json:"charger_actual_current"` // int
		ChargerPilotCurrent  int     `json:"charger_pilot_current"`  // int
		FastChargerPresent   bool    `json:"fast_charger_present"`   // bool
		FastChargerBrand     string  `json:"fast_charger_brand"`     // string
		FastChargerType      string  `json:"fast_charger_type"`      // string
		ConnChargeCable      string  `json:"conn_charge_cable"`      // string
	}
	// ClimateDetails struct - child of Positions
	type ClimateDetails struct {
		InsideTemp           float64 `json:"inside_temp"`            // float64
		OutsideTemp          float64 `json:"outside_temp"`           // float64
		IsClimateOn          bool    `json:"is_climate_on"`          // bool
		IsFrontDefrosterOn   bool    `json:"is_front_defroster_on"`  // bool
		IsRearDefrosterOn    bool    `json:"is_rear_defroster_on"`   // bool
		FanStatus            int     `json:"fan_status"`             // int
		DriverTempSetting    float64 `json:"driver_temp_setting"`    // float64
		PassengerTempSetting float64 `json:"passenger_temp_setting"` // float64
	}
	// VehicleDetails struct - child of Positions
	type VehicleDetails struct {
		Latitude          float64 `json:"latitude"`            // float64
		Longitude         float64 `json:"longitude"`           // float64
		Elevation         int     `json:"elevation"`           // int
		Speed             int     `json:"speed"`               // int
		Power             int     `json:"power"`               // int
		Odometer          float64 `json:"odometer"`            // float64
		IdealBatteryRange float64 `json:"ideal_battery_range"` // float64
		EstBatteryRange   float64 `json:"est_battery_range"`   // float64
		RatedBatteryRange float64 `json:"rated_battery_range"` // float64
		Heading           int     `json:"heading"`             // int
	}
	// Positions struct - child of Data
	type Positions struct {
		Date      string  `json:"date"`      // string, corresponds to 'time'
		Latitude  float64 `json:"latitude"`  // float64, avg(latitude)
		Longitude float64 `json:"longitude"` // float64, avg(longitude)
	}
	// TeslaMateUnits struct - child of Data
	type TeslaMateUnits struct {
		UnitsLength      string `json:"unit_of_length"`      // string
		UnitsTemperature string `json:"unit_of_temperature"` // string
	}
	// Data struct - child of JSONData
	type Data struct {
		Car            Car            `json:"car"`
		Positions      []Positions    `json:"positions"`
		TeslaMateUnits TeslaMateUnits `json:"units"`
	}
	// JSONData struct - main
	type JSONData struct {
		Data Data `json:"data"`
	}

	// creating required vars
	var (
		PositionsData []Positions
		CarData       Car
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
		SELECT
		date as time,
		avg(latitude) as latitude,
		avg(longitude) as longitude
		FROM
		positions p
		inner join cars c on c.id = p.car_id
		WHERE
		car_id = $1 and ideal_battery_range_km is not null
		`

	// Parameters to be passed to the query
	var queryParams []interface{}
	queryParams = append(queryParams, CarID)
	paramIndex := 2

	// Add date filtering if provided
	if parsedStartDate != "" {
		query += fmt.Sprintf(" AND date >= $%d", paramIndex)
		queryParams = append(queryParams, parsedStartDate)
		paramIndex++
	}
	if parsedEndDate != "" {
		query += fmt.Sprintf(" AND date <= $%d", paramIndex)
		queryParams = append(queryParams, parsedEndDate)
		paramIndex++
	}

	query += fmt.Sprintf(`
		GROUP BY 1
		ORDER BY 1 DESC
        LIMIT $%d OFFSET $%d;`, paramIndex, paramIndex+1)

	queryParams = append(queryParams, ResultShow, ResultPage)

	rows, err := db.Query(query, queryParams...)

	// checking for errors in query
	if err != nil {
		TeslaMateAPIHandleErrorResponse(c, "TeslaMateAPICarsPositionsV1", CarsPositionsError1, err.Error())
		return
	}

	// defer closing rows
	defer rows.Close()

	// looping through all results
	for rows.Next() {

		// creating position object based on struct
		position := Positions{}

		// scanning row and putting values into the position
		err = rows.Scan(
			&position.Date,
			&position.Latitude,
			&position.Longitude,
		)

		// checking for errors after scanning
		if err != nil {
			TeslaMateAPIHandleErrorResponse(c, "TeslaMateAPICarsPositionsV1", CarsPositionsError1, err.Error())
			return
		}

		// adjusting to timezone differences from UTC to be userspecific
		position.Date = getTimeInTimeZone(position.Date)

		// appending position to PositionsData
		PositionsData = append(PositionsData, position)
		CarData.CarID = CarID
	}

	// checking for errors in the rows result
	err = rows.Err()
	if err != nil {
		TeslaMateAPIHandleErrorResponse(c, "TeslaMateAPICarsPositionsV1", CarsPositionsError1, err.Error())
		return
	}

	//
	// build the data-blob
	jsonData := JSONData{
		Data{
			Car:       CarData,
			Positions: PositionsData,
		},
	}

	// return jsonData
	TeslaMateAPIHandleSuccessResponse(c, "TeslaMateAPICarsPositionsV1", jsonData)
}
