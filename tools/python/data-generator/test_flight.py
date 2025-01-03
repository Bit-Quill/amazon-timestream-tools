import pytest
import random
from datetime import datetime, timedelta
from flight_data_generator import FlightDataGenerator


@pytest.fixture
def generator():
    """Fixture to initialize FlightDataGenerator and common test parameters with a fixed random seed."""
    random.seed(42)  # Set a fixed seed for reproducibility
    gen = FlightDataGenerator()
    start_date = datetime(2024, 1, 1, 12, 0, 0)
    end_date = datetime(2024, 1, 1, 12, 10, 0) # 10 minutes
    reporting_frequency = timedelta(minutes=1)
    num_entities = 5
    return {
        "generator": gen,
        "start_date": start_date,
        "end_date": end_date,
        "reporting_frequency": reporting_frequency,
        "num_entities": num_entities,
    }


class TestFlightDataGenerator:
    def test_initialization(self, generator):
        """Test that the generator initializes airports, measure_templates, and dimension_templates correctly."""
        gen = generator["generator"]
        assert len(gen.airports) == 10, "Should have 10 airports initialized."
        assert len(gen.measure_templates) == 6, "Should have 6 measure templates."
        assert len(gen.dimension_templates) == 2, "Should have 2 dimension templates."

    def test_generate_records(self, generator):
        """
        Test the generate method to ensure it produces the correct number of records and entities.
        """
        gen = generator["generator"]
        start_date = generator["start_date"]
        end_date = generator["end_date"]
        reporting_frequency = generator["reporting_frequency"]
        num_entities = generator["num_entities"]

        records = gen.generate(
            start_date=start_date,
            end_date=end_date,
            reporting_frequency=reporting_frequency,
            num_entities=num_entities,
            precision="SECONDS",
            generate_unique_options_fallback=False
        )

        expected_num_records = 11 * num_entities # 11 minutes * num_entities
        assert len(records) == expected_num_records, f"Should generate {expected_num_records} records, got {len(records)}."

        # Check dimensions and measures in the first record
        first_record = records[0]
        assert len(first_record["Dimensions"]) == 2, f"Expected 2 dimensions, got {len(first_record['Dimensions'])}."
        assert len(first_record["Measures"]) == 6, f"Expected 6 measures, got {len(first_record['Measures'])}."

    def test_generate_exceeds_unique_flight_ids_without_fallback(self, generator):
        """
        Test that an exception is raised when num_entities exceeds available unique flight IDs
        and generate_unique_options_fallback is False.
        """
        gen = generator["generator"]
        start_date = generator["start_date"]
        end_date = generator["end_date"]
        reporting_frequency = generator["reporting_frequency"]

        num_unique_flight_ids = len(gen.dimension_templates[0]["unique_options"])
        with pytest.raises(Exception) as exc_info:
            gen.generate(
                start_date=start_date,
                end_date=end_date,
                reporting_frequency=reporting_frequency,
                num_entities=num_unique_flight_ids + 1,
                precision="SECONDS",
                generate_unique_options_fallback=False
            )
        assert "num_entities" in str(exc_info.value), "Exception should mention 'num_entities'."

    def test_generate_with_fallback(self, generator):
        """
        Test that generate_unique_options_fallback allows more entities than unique flight IDs.
        """
        gen = generator["generator"]
        start_date = generator["start_date"]
        end_date = generator["end_date"]
        reporting_frequency = generator["reporting_frequency"]

        num_unique_flight_ids = len(gen.dimension_templates[0]["unique_options"])
        num_entities = num_unique_flight_ids + 5  # Exceed unique flight IDs

        records = gen.generate(
            start_date=start_date,
            end_date=end_date,
            reporting_frequency=reporting_frequency,
            num_entities=num_entities,
            precision="SECONDS",
            generate_unique_options_fallback=True
        )

        expected_num_records = 11 * num_entities
        assert len(records) == expected_num_records, f"Should generate {expected_num_records} records with fallback, got {len(records)}."

    def test_calculate_bearing(self, generator):
        """Test the _calculate_bearing helper method."""
        gen = generator["generator"]
        lat1, lon1 = 33.6407, -84.4277  # Atlanta
        lat2, lon2 = 40.0799, 116.6031  # Beijing
        bearing = gen._calculate_bearing(lat1, lon1, lat2, lon2)
        # Bearing from Atlanta to Beijing is approximately 342 degrees
        assert abs(bearing - 342) <= 5, f"Bearing should be approximately 342 degrees, got {bearing} degrees."

    def test_update_entity_position(self, generator):
        """
        Test the _update_entity_position method to ensure entity positions are updated correctly.
        """
        gen = generator["generator"]
        reporting_frequency = timedelta(hours=1)
        knot_speed = 500

        entity = {
            "current_lat": 33.6407,
            "current_lon": -84.4277,
            "target_lat": 40.0799,
            "target_lon": 116.6031,
            "latest_measures": {}
        }

        # Simulate the position update
        updated_lat = gen._update_entity_position(
            entity, "lat", reporting_frequency, knot_speed
        )
        updated_lon = gen._update_entity_position(
            entity, "lon", reporting_frequency, knot_speed
        )

        # Check that the positions have been updated
        assert updated_lat != 33.6407, f"Latitude should be updated from 33.6407 to {updated_lat}."
        assert updated_lon != -84.4277, f"Longitude should be updated from -84.4277 to {updated_lon}."

    def test_generate_measure_value(self, generator):
        """
        Test the _generate_measure_value method for different measure types.
        """
        gen = generator["generator"]
        entity = {"latest_measures": {}}
        is_first_datapoint = True

        # Test initial fuel_level_gallons
        fuel_measure = next(m for m in gen.measure_templates if m["name"] == "fuel_level_gallons")
        value = gen._generate_measure_value(fuel_measure, entity, is_first_datapoint)
        assert value == 10000, "Initial fuel_level_gallons should be set to initial_value 10000."
        entity["latest_measures"]["fuel_level_gallons"] = value

        # Test subsequent fuel_level_gallons with possible variation
        is_first_datapoint = False
        value = gen._generate_measure_value(fuel_measure, entity, is_first_datapoint)
        # Expected to decrease by up to 80 (as per max_variation)
        assert 9920 <= value < 10000, f"fuel_level_gallons should decrease by up to 80 to between 9920 and 10000, got {value}."

        # Test speed_knots measure variance
        speed_measure = next(m for m in gen.measure_templates if m["name"] == "speed_knots")
        entity["latest_measures"]["speed_knots"] = 450
        value = gen._generate_measure_value(speed_measure, entity, is_first_datapoint=False)
        assert 440 <= value <= 460, f"speed_knots should vary within expected range (440-460), got {value}."
