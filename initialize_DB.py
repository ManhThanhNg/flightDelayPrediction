import psycopg2 as psycopg2

def create_table(conn, cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS delayedFlight (
            Year INTEGER,
            Month INTEGER,
            DayofMonth INTEGER,
            DayOfWeek INTEGER,
            DepTime FLOAT,
            CRSDepTime INTEGER,
            ArrTime FLOAT,
            CRSArrTime INTEGER,
            UniqueCarrier VARCHAR(255),
            FlightNum INTEGER,
            TailNum VARCHAR(255),
            ActualElapsedTime FLOAT,
            CRSElapsedTime FLOAT,
            AirTime FLOAT,
            ArrDelay FLOAT,
            DepDelay FLOAT,
            Origin VARCHAR(255),
            Dest VARCHAR(255),
            Distance INTEGER,
            TaxiIn FLOAT,
            TaxiOut FLOAT,
            Cancelled INTEGER,
            CancellationCode VARCHAR(255),
            Diverted INTEGER,
            CarrierDelay FLOAT,
            WeatherDelay FLOAT,
            NASDelay FLOAT,
            SecurityDelay FLOAT,
            LateAircraftDelay FLOAT
    );
            """
    )
    conn.commit()


if __name__ == "__main__":
    conn = psycopg2.connect("host=mthanh.ddns.net dbname=flight user=postgres password=postgres")
    cur = conn.cursor()
    create_table(conn, cur)
    cur.execute("SELECT * FROM delayedFlight")
    print(cur.fetchall())

    # # cur.execute("DROP TABLE delayedFlight")
    # conn.commit()