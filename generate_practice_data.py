
import pandas as pd
import numpy as np
import random
import os

def generate_telecom_churn_data(n_samples=2000, random_seed=42):
    np.random.seed(random_seed)
    random.seed(random_seed)

    # Helper functions
    def random_choice(choices, p=None):
        return np.random.choice(choices, n_samples, p=p)

    # 1. Customer Demographics
    customer_ids = [f'CUST-{i:05d}' for i in range(1, n_samples + 1)]
    gender = random_choice(['Male', 'Female'])
    senior_citizen = random_choice([0, 1], p=[0.85, 0.15]) # 15% are seniors
    partner = random_choice(['Yes', 'No'], p=[0.48, 0.52])
    dependents = random_choice(['Yes', 'No'], p=[0.30, 0.70])

    # 2. Service Usage & Logic
    tenure = np.random.randint(1, 72, n_samples) # Months as customer

    # Phone Service
    phone_service = random_choice(['Yes', 'No'], p=[0.9, 0.1])
    # MultipleLines only if PhoneService is Yes. If No, it should be 'No phone service'
    multiple_lines_raw = random_choice(['Yes', 'No'], p=[0.4, 0.6])
    multiple_lines = np.where(phone_service == 'Yes', multiple_lines_raw, 'No phone service')

    # Internet Service
    internet_service = random_choice(['DSL', 'Fiber optic', 'No'], p=[0.35, 0.40, 0.25])
    
    # Add-on Services (only if internet service != No)
    def addon_service(name, prob_yes):
        raw = random_choice(['Yes', 'No'], p=[prob_yes, 1-prob_yes])
        return np.where(internet_service != 'No', raw, 'No internet service')

    online_security = addon_service('OnlineSecurity', 0.3)
    online_backup = addon_service('OnlineBackup', 0.35)
    device_protection = addon_service('DeviceProtection', 0.3)
    tech_support = addon_service('TechSupport', 0.3)
    streaming_tv = addon_service('StreamingTV', 0.4)
    streaming_movies = addon_service('StreamingMovies', 0.4)

    # 3. Account Information
    contract = random_choice(['Month-to-month', 'One year', 'Two year'], p=[0.55, 0.25, 0.20])
    paperless_billing = random_choice(['Yes', 'No'], p=[0.6, 0.4])
    payment_method = random_choice(['Electronic check', 'Mailed check', 'Bank transfer (automatic)', 'Credit card (automatic)'])

    # 4. Charges Calculation (Approximate logic)
    # Base charge around 20 for basic service
    monthly_charges = np.random.normal(20, 5, n_samples)
    
    # Add costs for services
    monthly_charges += np.where(phone_service == 'Yes', 20, 0)
    monthly_charges += np.where(multiple_lines == 'Yes', 10, 0)
    monthly_charges += np.where(internet_service == 'Fiber optic', 30, (np.where(internet_service == 'DSL', 15, 0)))
    monthly_charges += np.where(online_security == 'Yes', 5, 0)
    monthly_charges += np.where(online_backup == 'Yes', 5, 0)
    monthly_charges += np.where(device_protection == 'Yes', 5, 0)
    monthly_charges += np.where(tech_support == 'Yes', 5, 0)
    monthly_charges += np.where(streaming_tv == 'Yes', 10, 0)
    monthly_charges += np.where(streaming_movies == 'Yes', 10, 0)

    # Add randomness
    monthly_charges += np.random.normal(0, 2, n_samples)
    monthly_charges = np.maximum(monthly_charges, 18.25).round(2)

    total_charges = (monthly_charges * tenure)
    # Add some noise to total charges
    total_charges += np.random.normal(0, 10, n_samples)
    total_charges = np.abs(total_charges).round(2)

    # 5. Churn Logic (Creating patterns for the user to find)
    churn_prob = np.zeros(n_samples) + 0.15 # Base churn

    # Factors increasing churn
    churn_prob[contract == 'Month-to-month'] += 0.4
    churn_prob[internet_service == 'Fiber optic'] += 0.15
    churn_prob[payment_method == 'Electronic check'] += 0.1
    churn_prob[paperless_billing == 'Yes'] += 0.05
    churn_prob[senior_citizen == 1] += 0.1

    # Factors decreasing churn
    churn_prob[tenure > 24] -= 0.1
    churn_prob[tenure > 48] -= 0.1
    churn_prob[contract == 'Two year'] -= 0.3
    churn_prob[tech_support == 'Yes'] -= 0.15
    churn_prob[online_security == 'Yes'] -= 0.1
    churn_prob[dependents == 'Yes'] -= 0.05
    churn_prob[partner == 'Yes'] -= 0.05

    # Clip probabilities
    churn_prob = np.clip(churn_prob, 0, 1)

    # Generate Churn labels
    churn = ['Yes' if p > random.random() else 'No' for p in churn_prob]

    # 6. Introduce Data Quality Issues
    
    # Missing Values in TotalCharges
    # Randomly set some TotalCharges to NaN (e.g., new customers or errors)
    nan_indices = np.random.choice(range(n_samples), size=int(n_samples * 0.015), replace=False)
    # Pandas to_csv handles NaN as empty string if not specified, but let's keep it empty or NaN consistent
    # We will use pandas to assign np.nan
    # But wait, total_charges is numpy array.
    total_charges[nan_indices] = np.nan

    # Create DataFrame
    df = pd.DataFrame({
        'customerID': customer_ids,
        'gender': gender,
        'SeniorCitizen': senior_citizen,
        'Partner': partner,
        'Dependents': dependents,
        'tenure': tenure,
        'PhoneService': phone_service,
        'MultipleLines': multiple_lines,
        'InternetService': internet_service,
        'OnlineSecurity': online_security,
        'OnlineBackup': online_backup,
        'DeviceProtection': device_protection,
        'TechSupport': tech_support,
        'StreamingTV': streaming_tv,
        'StreamingMovies': streaming_movies,
        'Contract': contract,
        'PaperlessBilling': paperless_billing,
        'PaymentMethod': payment_method,
        'MonthlyCharges': monthly_charges,
        'TotalCharges': total_charges,
        'Churn': churn
    })

    return df

if __name__ == "__main__":
    if not os.path.exists('data'):
        os.makedirs('data')
    
    print("Generating dataset...")
    df = generate_telecom_churn_data(n_samples=3000)
    
    output_path = os.path.join("data", "telecom_customer_churn_practice.csv")
    df.to_csv(output_path, index=False)
    print(f"Dataset generated at: {output_path}")
    print(f"Shape: {df.shape}")
    print("Columns:", df.columns.tolist())
