# imported for adding colors to console outputs
from colorama import init, Fore, Back, Style

# Initialize colorama for cross-platform color support (e.g., Windows, macOS, Linux)
init()

class ConsoleFormatter: # provide reusable methods for consistent and styled console outputs
    @staticmethod
    def header(text):
        """Format text as a header with cyan color"""
        return f"{Fore.CYAN}{Style.BRIGHT}=== {text} ==={Style.RESET_ALL}"
    
    @staticmethod
    def success(text):
        """Format text as a success message with green color"""
        return f"{Fore.GREEN}✓ {text}{Style.RESET_ALL}"
    
    @staticmethod
    def error(text):
        """Format text as an error message with red color"""
        return f"{Fore.RED}✗ {text}{Style.RESET_ALL}"
    
    @staticmethod
    def warning(text):
        """Format text as a warning message with yellow color"""
        return f"{Fore.YELLOW}⚠ {text}{Style.RESET_ALL}"
    
    @staticmethod
    def info(text):
        """Format text as an info message with blue color"""
        return f"{Fore.BLUE}ℹ {text}{Style.RESET_ALL}"
    
    @staticmethod
    def highlight(text):
        """Format text as highlighted with magenta color"""
        return f"{Fore.MAGENTA}{text}{Style.RESET_ALL}"
    
    @staticmethod
    def table_row(columns, widths):
        """Format a table row with specified column widths"""
        return "".join(str(col).ljust(width) for col, width in zip(columns, widths))
    
    @staticmethod
    def separator(char="-", length=50):
        """Create a separator line with specified character and length"""
        return f"{Fore.BLUE}{char * length}{Style.RESET_ALL}" 

# # those print is only for demo
# print(ConsoleFormatter.header("Database Query Tool"))
# print(ConsoleFormatter.info("Connecting to database..."))
# print(ConsoleFormatter.success("Connection successful!"))
# print(ConsoleFormatter.warning("Low memory warning"))
# print(ConsoleFormatter.error("Query failed!"))
# print(ConsoleFormatter.highlight("Critical Update"))
# print(ConsoleFormatter.table_row(["Name", "Age"], [10, 5]))
# print(ConsoleFormatter.separator())

