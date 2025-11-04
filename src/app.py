import streamlit as st
import joblib
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from utils.frontend_utils import get_cities, heating_types, categorize_city, load_data

project_root = Path(__file__).resolve().parent.parent

model1_path = project_root / 'notebooks' / 'XGBoost' / '20251029_171626_apartment_valuation_model.pk1'
model2_path = project_root / 'notebooks' / 'XGBoost' / '20251029_171626_house_valuation_model.pk1'

def load_models():
    try:
        model_apts = joblib.load(model1_path)
        model_houses = joblib.load(model2_path)
        return model_apts, model_houses
    except Exception as e:
        st.error(f"Error loading models: {e}")
        return None, None

def estimation_page():
    st.set_page_config(layout="centered")
    st.title("Real Estate Valuation")
    st.header("Enter Property Details")

    type_of_property = st.radio("Select Property Type", ["Apartment", "House"], horizontal=True)
    if type_of_property == "Apartment":
        col1, col2, col3= st.columns(3)
        with col1:
            size = st.number_input("Size (m²)", min_value=10, max_value=1000, value=50)
        with col2:
            rooms = st.number_input("Number of Rooms", min_value=1, max_value=20, value=3)
        with col3:
            bathrooms = st.number_input("Number of Bathrooms", min_value=1, max_value=10, value=1)
        col1, col2, col3 = st.columns(3)
        with col1:
            condition = st.selectbox("Condition", ["newly built", "excellent", "good", "average", "bad"])
        with col2:
            facade_condition = st.selectbox("Facade Condition", ["newly built", "excellent", "good", "average", "bad"])
        with col3:
            stairwell_condition = st.selectbox("Stairwell Condition", ["newly built", "excellent", "good", "average", "bad"])
        
    elif type_of_property == "House":
        col1, col2 = st.columns(2)
        with col1:
            size = st.number_input("Size (m²)", min_value=10, max_value=1000, value=50)
        with col2:
            property_size = st.number_input("Property Size (m²)", min_value=10, max_value=5000, value=200)
            property_log_size = np.log(property_size)
        col1, col2 = st.columns(2)
        with col1:
            rooms = st.number_input("Number of Rooms", min_value=1, max_value=20, value=3)
        with col2:
            bathrooms = st.number_input("Number of Bathrooms", min_value=1, max_value=10, value=1)
        col1, col2 = st.columns(2)
        with col1:
            condition = st.selectbox("Condition", ["newly built", "excellent", "good", "average", "bad"])
        with col2:
            facade_condition = st.selectbox("Facade Condition", ["newly built", "excellent", "good", "average", "bad"])
    
    log_size = np.log(size)
    heating_type_options = heating_types(type_of_property)
    heating = st.selectbox("Heating Type", heating_type_options)

    _, _, _, city_options = get_cities()
    city = st.selectbox("City", city_options + ["other"])
    city_type = categorize_city(city)
    
    col1, col2 = st.columns(2)
    with col1:
        age_of_property = st.number_input("Age of Property (years)", min_value=0, max_value=100, value=10)
    with col2:
        legal_status = st.selectbox("Legal Status", ["new", "used"])

    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        estimate_button =  st.button("Estimate Value")
    with col2:
        debug_checkbox = st.checkbox("Show input data")

    if estimate_button:
        if type_of_property == "Apartment":
            model, input_data = get_apartment_input_data(log_size, rooms, bathrooms, condition, facade_condition, stairwell_condition, heating, city, city_type, age_of_property, legal_status, size)
            if debug_checkbox:
                debug_input(input_data)
            predict_value(model, input_data)
        elif type_of_property == "House":
            model, input_data = get_house_input_data(log_size, rooms, bathrooms, condition, facade_condition, heating, city, city_type, age_of_property, legal_status, size, property_log_size)
            if debug_checkbox:
                debug_input(input_data)
            predict_value(model, input_data)

def debug_input(input_data):
    st.write(input_data)

def get_house_input_data(log_size, rooms, bathrooms, condition, facade_condition, heating, city, city_type, age_of_property, legal_status, size, property_log_size):
    _, model = load_models()
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
        'city_district': [city],
        'city_type': [city_type],
        'age_of_property': [age_of_property],
        'legal_status': [legal_status]
    })

    return model, input_data

def get_apartment_input_data(log_size, rooms, bathrooms, condition, facade_condition, stairwell_condition, heating, city, city_type, age_of_property, legal_status, size):
    model, _ = load_models()
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
        'city_district': [city],
        'city_type': [city_type],
        'age_of_property': [age_of_property],
        'legal_status': [legal_status]
    })

    return model, input_data

def predict_value(model, input_data):
    try:
        prediction = model.predict(input_data)[0]
        price = np.exp(prediction)
        st.success(f"Estimated Property Value: {price:,.0f} HUF")
    except Exception as e:
        st.error(f"Error during prediction: {e}")
        print(f"Error during prediction: {e}")

def data_visualization_page():
    st.set_page_config(layout="wide")
    st.title("Data Visualization")

    houses, apartments = load_data()

    sample_size = min(len(houses), len(apartments), 3000)
    houses = houses.sample(n=sample_size, random_state=42)
    apartments = apartments.sample(n=sample_size, random_state=42)

    tab1, tab2 = st.tabs(["Apartments vs. Houses", "Data Exploreer"])
    with tab1:
        st.header("Apartments vs. Houses")

        col1, col2 = st.columns(2)
        with col1:
                st.subheader("Apartments")

                fig = px.histogram(apartments, x='price', title='Apartment Price Distribution', range_x=[0, 3e8], labels={'price':'Price (HUF)'})
                st.plotly_chart(fig, use_container_width=True)

                fig = go.Figure(go.Histogram(x=apartments["size"], xbins=dict(start=0, end=500, size=10), hovertemplate='Count: %{y}<br>Size: %{x} sqm<extra></extra>'))
                fig.update_layout(title="Property Size Distribution", xaxis_title="Size (sqm)", yaxis_title="Count", xaxis=dict(range=[0, 500]), yaxis=dict(range=[0, 400]))
                st.plotly_chart(fig, use_container_width=True)

                fig = px.box(apartments, x='city_type', y='price', title='Apartment Price by City Type', range_y=[0, 4e8], labels={'price':'Price (HUF)', 'city_type':'City Type'})
                st.plotly_chart(fig, use_container_width=True)

                fig = px.scatter(apartments, x='price', y='size', trendline="ols", title='Apartment Price vs Size', labels={'price':'Price (HUF)', 'size':'Size (sqm)'}, range_x=[0, 5e8], range_y=[0, 500])
                fig.update_traces(marker=dict(size=8, opacity=0.6), selector=dict(mode='markers'))
                fig.update_traces(line=dict(color='red', width=4), selector=dict(mode='lines'))
                st.plotly_chart(fig, use_container_width=True)

                # 

        with col2:
                st.subheader("Houses")

                fig = px.histogram(houses, x='price', title='House Price Distribution', range_x=[0, 3e8], labels={'price':'Price (HUF)'})
                st.plotly_chart(fig, use_container_width=True)

                fig = go.Figure(go.Histogram(x=houses["size"], xbins=dict(start=0, end=500, size=10), hovertemplate='Count: %{y}<br>Size: %{x} sqm<extra></extra>'))
                fig.update_layout(title="Property Size Distribution", xaxis_title="Size (sqm)", yaxis_title="Count", xaxis=dict(range=[0, 500]), yaxis=dict(range=[0, 400]))
                st.plotly_chart(fig, use_container_width=True)

                fig = px.box(houses, x='city_type', y='price', title='House Price by City Type', range_y=[0, 4e8], labels={'price':'Price (HUF)', 'city_type':'City Type'})
                st.plotly_chart(fig, use_container_width=True)

                fig = px.scatter(houses, x='price', y='size', trendline="ols", title='House Price vs Size', labels={'price':'Price (HUF)', 'size':'Size (sqm)'}, range_x=[0, 5e8], range_y=[0, 500])
                fig.update_traces(marker=dict(size=8, opacity=0.6), selector=dict(mode='markers'))
                fig.update_traces(line=dict(color='red', width=4), selector=dict(mode='lines'))
                st.plotly_chart(fig, use_container_width=True)

    with tab2:
        st.header("Data Explorer")
        st.subheader("Filters")
        apartments['property_type'] = 'Apartment'
        houses['property_type'] = 'House'
        df = pd.concat([apartments, houses])
        selected_property_type = st.multiselect("Select Property Type", options=['Apartment', 'House'], default=['Apartment', 'House'])
        selected_city = st.multiselect("Select City", options=houses['city'].unique().tolist() + apartments['city'].unique().tolist(), default=houses['city'].unique().tolist()[:5])
        selected_price_range = st.slider("Select Price Range (HUF)", min_value=0, max_value=1_000_000_000, value=(0, 500_000_000), step=1_000_000)
        selected_size_range = st.slider("Select Size Range (sqm)", min_value=0, max_value=2000, value=(0, 500), step=10)

        filtered_df = df[
            (df['property_type'].isin(selected_property_type)) &
            (df['city'].isin(selected_city)) &
            (df['price'].between(*selected_price_range)) &
            (df['size'].between(*selected_size_range))
        ]

        st.write("Total Properties:", filtered_df.shape[0])

        if st.checkbox("Show Filtered Data"):
            st.dataframe(filtered_df)

        if not filtered_df.empty:
            col1, col2 = st.columns(2)
            with col1:
                fig = px.histogram(filtered_df, x='price', color='property_type', barmode='overlay', title='Price Distribution', range_x=[0, 1e9], labels={'price':'Price (HUF)', 'property_type':'Property Type'})
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                fig = px.histogram(filtered_df, x='size', color='property_type', barmode='overlay', title='Size Distribution', range_x=[0, 2000], labels={'size':'Size (sqm)', 'property_type':'Property Type'})
                st.plotly_chart(fig, use_container_width=True)
            

def model_performance_page():
    st.title("Model Performance")
    st.header("Evaluate the Model")

def sidebar_navigation():
    page = st.sidebar.radio(
        "Go to", 
        ["Estimate Property Value", "Data Visualization", "Model Performance"]
    )
    return page

def main():
    model_apts, model_houses = load_models()
    page = sidebar_navigation()
    if page == "Estimate Property Value":
        estimation_page()
    elif page == "Data Visualization":
        data_visualization_page()
    elif page == "Model Performance":
        model_performance_page()

if __name__ == "__main__":
    main()