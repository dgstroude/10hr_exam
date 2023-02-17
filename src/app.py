from flask import Flask, request, jsonify

# create the Flask app
app = Flask(__name__)

@app.route('/api/weather', methods=['GET'])
def get_daily_records():
    import json
    from weather_station_records import WeatherStationDailyRecord

    weather_station_id = request.args.get('station_id')
    record_date = str(request.args.get('date'))
    try:
        weather_station = WeatherStationDailyRecord()
        weather_station.initialize()
    except Exception as e:
        print(f"Error(s) when working with WeatherStationDailyRecord object: {str(e)}")
    
    select_query = (f"SELECT * FROM {WeatherStationDailyRecord.WEATHER_STATION_DAILY_RECORD} WHERE "
                    f"weather_station_id = '{weather_station_id}' AND "
                    f"record_date = '{record_date}'"
                   )   

    try:
        cursor = weather_station.db.get_cursor()
        cursor.execute(select_query)

    except Exception as e:
        print(f"Error while querying db: {str(e)}")
        cursor.close()
        return
    rows = cursor.fetchall()
    json_rows = json.dumps(rows, default=str) # 'default=str' is needed to convert non-serializable objects, such as a date into a string here
    cursor.close()
    return jsonify(rows)


@app.route('/api/weather/stats', methods=['GET'])
def get_annual_records():
    import json
    from weather_station_records import WeatherStationAnnualRecord

    weather_station_id = request.args.get('station_id')
    record_year = request.args.get('year')
    try:
        weather_station = WeatherStationAnnualRecord()
        weather_station.initialize()
    except Exception as e:
        print(f"Error(s) when working with WeatherStationAnnualRecord object: {str(e)}")

    select_query = (f"SELECT * FROM {WeatherStationAnnualRecord.WEATHER_STATION_ANNUAL_RECORD} WHERE "
                    f"weather_station_id = '{weather_station_id}' AND "
                    f"year = '{record_year}'"
                   )   
    try:
        cursor = weather_station.db.get_cursor()
        cursor.execute(select_query)
    except Exception as e:
        print(f"Error while querying db: {str(e)}")
        cursor.close()
        return
    
    rows = cursor.fetchall()
    json_rows = json.dumps(rows)
    cursor.close()
    #return flask.jsonify("<p>Hello, Annual Rows! {json_rows}</p>"
    return jsonify(rows)
