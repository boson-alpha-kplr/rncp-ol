# SET UP
# First, we must upgrade pip and install all the dependencies.
echo "#Install and upgrade Python dependencies"
pip install --upgrade pip
pip3 install -r requirements.txt

# We then must retrieve the notebook's IP address and white list in Microsoft Azure to allow the subsequent script to access our PostgreSQL database hosted on Azure.
echo "#Retrieve public IP address"
python retrieve_ip.py
echo "You should white list the public IP adress on the database server before proceding further.
When you're done, please press 'Enter'."
read input_check

echo "Everything's fine !"