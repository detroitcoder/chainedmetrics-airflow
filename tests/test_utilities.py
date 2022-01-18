from utilities import email_table_template

def test_email_table_template():

    email = email_table_template(
        'Test Subject', "Table description sentence", 
        headers=["a", "b", "c"],
        rows=[[1,2,3], [4, 5, 6]]
    )