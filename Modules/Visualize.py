import Load as load

class Visualize:

    def __init__(self, dbConnection):
        """
        Initializes the TripDataLoader with a database connection object.

        Args:
            dbConnection (dBConnect): A connected dBConnect instance.
        """
        self.db = dbConnection