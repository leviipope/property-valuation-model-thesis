import streamlit as st
import joblib
import pandas as pd
import numpy as np
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent

model1_path = project_root / 'notebooks' / 'XGBoost' / '20251029_171626_apartment_valuation_model.pk1'
model2_path = project_root / 'notebooks' / 'XGBoost' / '20251029_171626_house_valuation_model.pk1'

try:
    model1 = joblib.load(model1_path)
    model2 = joblib.load(model2_path)
except Exception as e:
    print(f"Error loading models: {e}")

st.title("Real Estate Valuation Demo")

st.header("Enter Property Details")

capital_city = 'budapest'
large_cities = ['debrecen', 'szeged', 'miskolc', 'győr', 'pécs']
big_cities = ['nyíregyháza', 'kecskemét', 'székesfehérvár', 'szombathely', 'érd', 'szolnok', 'tatabánya', 'kaposvár', 'békéscsaba', 'veszprém', 'zalaegerszeg', 'eger', 'nagykanizsa']
city_list = [capital_city] + large_cities + big_cities

type_of_property = st.selectbox("Type of Property", ["Apartment", "House"])
size = st.number_input("Size (m²)", min_value=10, max_value=1000, value=50)
log_size = np.log(size)
if type_of_property == "House":
    property_size = st.number_input("Property Size (m²)", min_value=10, max_value=5000, value=200)
    property_log_size = np.log(property_size)
rooms = st.number_input("Number of Rooms", min_value=1, max_value=20, value=3)
bathrooms = st.number_input("Number of Bathrooms", min_value=1, max_value=10, value=1)
condition = st.selectbox("Condition", ["newly built", "excellent", "good", "average", "bad"])
facade_condition = st.selectbox("Facade Condition", ["newly built", "excellent", "good", "average", "bad"])
if type_of_property == "Apartment":
    stairwell_condition = st.selectbox("Stairwell Condition", ["newly built", "excellent", "good", "average", "bad"])
heating = st.selectbox("Heating Type", ["district heating", "central gas heating", "individual district heating", "gas convector", "heat pump", "electric", "other"])
city = st.selectbox("City", city_list)
city_district = city
city_type = st.selectbox("City Type", [1, 2, 3 , 4])
age_of_property = st.number_input("Age of Property (years)", min_value=0, max_value=100, value=10)
legal_status = st.selectbox("Legal Status", ["new", "used"])


if st.button("Estimate Value"):
    if type_of_property == "Apartment":
        model = model1
        input_data = pd.DataFrame({
            'log_size': [log_size],
            'rooms': [rooms],
            'size_per_room': [size / rooms],
            'bathrooms': [bathrooms],
            'bathrooms_per_room': [bathrooms / rooms],
            'condition': [condition],
            'facade_condition': [facade_condition],
            'stairwell_condition': [stairwell_condition],
            'heating': [heating],
            'city': [city],
            'city_district': [city_district],
            'city_type': [city_type],
            'age_of_property': [age_of_property],
            'legal_status': [legal_status]
        })

    else:
        model = model2
        input_data = pd.DataFrame({
            'log_size': [log_size],
            'log_property_size': [property_log_size],
            'rooms': [rooms],
            'size_per_room': [size / rooms],
            'bathrooms': [bathrooms],
            'bathrooms_per_room': [bathrooms / rooms],
            'condition': [condition],
            'facade_condition': [facade_condition],
            'heating': [heating],
            'city': [city],
            'city_district': [city_district],
            'city_type': [city_type],
            'age_of_property': [age_of_property],
            'legal_status': [legal_status]
        })
        
    try:
        prediction = model.predict(input_data)[0]
        price = np.exp(prediction)
        st.success(f"Estimated Property Value: {price:,.0f} HUF")
    except Exception as e:
        st.error(f"Error during prediction: {e}")
        print(f"Error during prediction: {e}")