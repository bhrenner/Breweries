import smtplib

def send_test_email():
    smtp_server = "smtp-mail.outlook.com"
    smtp_port = 587
    smtp_user = "breweriescase@hotmail.com"
    smtp_password = "breCase#072024"
    
    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(smtp_user, "bhrenner_wilson@hotmail.com", "Test email from Airflow")
        print("Email sent successfully!")
    except Exception as e:
        print(f"Failed to send email: {e}")

send_test_email()
