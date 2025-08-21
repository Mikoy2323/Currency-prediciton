# 💱 Currency Prediction System  

A **currency forecasting system** that predicts future exchange rates for multiple currencies and provides **investment recommendations** based on model confidence.  

## 🚀 Overview  
This project uses **time series forecasting** to predict future values of various currencies. For each currency, a **dedicated model** is trained using its historical data. Predictions are updated daily and visualized through an interactive **Power BI dashboard**.  

### Key Features  
✅ **Separate model per currency** – each currency has its own time-series prediction model.  
✅ **Daily automated updates** – a script retrains models and generates fresh forecasts every day.  
✅ **Investment recommendations** – highlights currencies that might be worth investing in based on predictions.  
✅ **Power BI integration** – all forecasts and historical data are connected to a Power BI dashboard for interactive analysis.  
✅ **Risk Indicator (`Współczynnik ryzyka`)** – based on **RMAPE (Relative Mean Absolute Percentage Error)**, which measures prediction accuracy:  
   - **Lower score = better prediction (lower risk)**  
   - **Higher score = worse prediction (higher risk)**  

## 📊 How It Works  
1. Collect historical exchange rate data for each currency.  
2. Train an individual **time series model** for each currency.  
3. Generate **future predictions** for each currency.  
4. Calculate **RMAPE** as a confidence indicator for each model.  
5. Update results daily via an automated script.  
6. Visualize and analyze everything in **Power BI**.  

---

📄 **Full system description** can be found in the file: **`dokumentacja.pdf`**
