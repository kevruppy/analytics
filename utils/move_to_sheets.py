import os
import gspread


class GSheetManager:
    """TBD"""

    def __init__(self):
        self.gc = gspread.service_account(os.getenv("GCP_SERVICE_ACCOUNT"))

    def create_spreadsheet(self, title):
        """Creates a new blank spreadsheet with the given title.

        Args:
            title (str): The title of the new spreadsheet.

        Returns:
            gspread.models.Spreadsheet: The created spreadsheet object.
        """
        return self.gc.create(title)

    def add_worksheet(self, spreadsheet, title):
        """Adds a new worksheet to the given spreadsheet with the specified title.

        Args:
            spreadsheet (gspread.models.Spreadsheet): The spreadsheet object.
            title (str): The title of the new worksheet.
        """
        spreadsheet.add_worksheet(title=title)

    def share_spreadsheet(self, spreadsheet):
        """Shares the given spreadsheet with the specified email address and role.

        Args:
            spreadsheet (gspread.models.Spreadsheet): The spreadsheet object.
        """
        spreadsheet.share(
            email=os.getenv("GOOGLE_MAIL_ADDRESS"), perm_type="user", role="writer"
        )

    def open_spreadsheet(self, spreadsheet):
        """TBD"""

        return self.gc.open(spreadsheet)

    def df2worksheet(self, spreadsheet):
        """Shares the given spreadsheet with the specified email address and role.

        Args:
            spreadsheet (gspread.models.Spreadsheet): The spreadsheet object.
        """
        spreadsheet.share(
            email=os.getenv("GOOGLE_MAIL_ADDRESS"), perm_type="user", role="writer"
        )


# sh = gc.open("MARTS")

# sh.worksheet("INPUT").clear()

# con = duckdb.connect("/workspaces/analytics/analytics.duckdb")

# result = con.execute(qry)

# df = result.df()

# worksheet = sh.worksheet("INPUT")
# worksheet.update([df.columns.values.tolist()] + df.values.tolist())
