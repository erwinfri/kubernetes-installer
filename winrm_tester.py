import winrm
import sys

# Usage: python winrm_tester.py <host> <username> <password>
# Example: python winrm_tester.py localhost Administrator SecureP@ssw0rd!

def main():
    if len(sys.argv) != 4:
        print("Usage: python winrm_tester.py <host> <username> <password>")
        sys.exit(1)
    host = sys.argv[1]
    username = sys.argv[2]
    password = sys.argv[3]
    url = f'http://{host}:5985/wsman'
    try:
        print(f"Connecting to {url} as {username}")
        s = winrm.Session(url, auth=(username, password))
        print("Session created. Sending command: whoami")
        r = s.run_cmd('whoami')
        print(f"Status code: {r.status_code}")
        print("STDOUT:")
        print(r.std_out.decode())
        print("STDERR:")
        print(r.std_err.decode())
        print("Full response object:")
        print(r)
        if hasattr(r, 'std_out'):
            print("Raw STDOUT bytes:", r.std_out)
        if hasattr(r, 'std_err'):
            print("Raw STDERR bytes:", r.std_err)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
