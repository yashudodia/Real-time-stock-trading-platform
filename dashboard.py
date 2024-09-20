import streamlit as st
import pandas as pd
import plotly.graph_objs as go
from prediction import predict_stock_prices, spark,F  # Ensure your prediction function and Spark session are defined
# import streaming_kafka
def creds_entered():
    if st.session_state['user'].strip() =="admin" and st.session_state["passwd"].strip() =="admin":
        st.session_state['authenticated'] = True
    else:
        st.session_state['authenticated'] = False
        if not st.session_state["passwd"]:
            st.warning("Please Password")
        elif not st.session_state["user"]:
            st.warning("Please enter username")
        else:
            st.error("Invalid Username/Password :face_with_raised_eyebrow:")    
def authenticate_user():
    if "authenticated" not in st.session_state:
        st.text_input(label = "Username : ", value = "",key = "user", on_change = creds_entered)
        st.text_input(label = "Password : ", value = "",key = "passwd",type = "password",on_change = creds_entered)
        return False
    else:

        if st.session_state["authenticated"]:
            return True
        else:
            st.text_input(label = "Username : ", value = "",key = "user", on_change = creds_entered)
            st.text_input(label = "Password : ", value = "",key = "passwd",type = "password",on_change = creds_entered)
            return False


def plot_it(ticker):
    # predictions = predict_stock_prices(ticker)
            try:    # streaming_kafka.fetch_and_stream_historical_prices()
                predictions, training_data = predict_stock_prices(ticker)
                training_data = training_data.filter(F.col("Date") > '2022-04-11') 
                training_data_df = training_data.toPandas()
                training_data_df['Date'] = pd.to_datetime(training_data_df['Date'])

                # Convert Spark DataFrame to Pandas DataFrame for display in Streamlit
                predictions_df = predictions.toPandas()
                predictions_df['Date'] = pd.to_datetime(predictions_df['Date'])  # Ensure Date is in datetime format

                # Display the predictions and actual prices
                st.write(f"Predictions for {ticker}:")
                st.dataframe(predictions_df[['Date', 'label', 'prediction']])

                
                # Create a Plotly graph
                fig = go.Figure()
                # Historical data plot
                fig.add_trace(go.Scatter(x=training_data_df['Date'], y=training_data_df['Close'],
                                        mode='lines', name='Historical Closing Price',
                                        line=dict(color='grey', dash='dot')))
                # Predicted data plot
                fig.add_trace(go.Scatter(x=predictions_df['Date'], y=predictions_df['label'],
                                        mode='lines', name='Actual Price',
                                        line=dict(color='blue')))
                fig.add_trace(go.Scatter(x=predictions_df['Date'], y=predictions_df['prediction'],
                                        mode='lines', name='Predicted Price',
                                        line=dict(color='red')))

                fig.update_layout(title='Stock Price Prediction',
                                xaxis_title='Date',
                                yaxis_title='Price',
                                legend_title='Legend',
                                xaxis_rangeslider_visible = True)

                # Display the Plotly graph
                st.plotly_chart(fig)
            except Exception as e:
                st.error(f"Failed to predict stock prices: {e}")
       
def main():
    if authenticate_user():
        st.title('Stock Price Prediction Dashboard')

        # # User input for the ticker symbol
        # ticker = st.text_input("Enter the stock ticker symbol (e.g., AAPL, TSLA):", "AAPL")

        # Available tickers dropdown
        available_tickers = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'GL', 'AMZN','NVDA']  # Add more tickers as needed
        ticker = st.selectbox("Select the stock ticker:", available_tickers)

        
        if st.button("Predict Stock Prices"):
            # Call your prediction function
            plot_it(ticker)

if __name__ == "__main__":
    main()
