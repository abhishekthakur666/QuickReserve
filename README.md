# QuickReserve

* PYTHON ASYNCIO Based - Fast and efficient car reservation system with great User Experience for manager and customer roles.
* Main features
    - Operator Management to create Manager and Customer User
    - RBAC for different type of users
    - Register Car / Modify Car details / Unregister car
    - Reserved available cars
    - DB Worker pool for concurrent DB Access
    - Unique and non-unique index support for faster db access 
    
   
  * How to install run 
    - Ensure Python3.9+ is installed
    - Ensure pyreadline, prettytable and termcolor libraries are installed
    - Clone the code
    - python reservecli.py 

* Output (By default Master CLI prompt will be launched)
    - Master admin can create customer operator
        - CMD -  master:abhishek@qr.com#register operator email_address=ravi@qr.com role=customer
    - Master admin can set customer operator password
        - CMD - register op_credentials operator_email=ravi@qr.com password=test1234
    - Master admin can create manager operator
        - CMD - register operator email_address=sagar@qr.com role=manager
    - Master admin can set manager operator password
        - CMD - register op_credentials operator_email=sagar@qr.com password=test1234
    - Login as Manager
        - CMD - login email_address=sagar@qr.com password=test1234
    - register new car
        - CMD - register car model_name=Tesla reg_no=12345
     - Login as Customer
        - CMD - login email_address=ravi@qr.com password=test1234
     - Show cars by model
        - CMD - show car model_name=Tesla
    - Reserve car
        - CMD - reserve car_reg_no=12345
    - Inspect car reservations (Applicable for both manager and customer)
        - CMD - inspect_reservation model_name=Tesla
      


