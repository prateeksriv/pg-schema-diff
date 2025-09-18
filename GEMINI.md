# Building

If a Makefile exists, refer to it for building, installing, etc.

# Running the program after building

To test this project, the command is: make test
This is not a unit test but a test against my sample data.
Do not go and run any tests other than the command specified in "make test"

# Logging

Use the logging functions in pkg/log/logger.go to implement any logging. 
You can use fmt.Fprintf to get assurance temporarily but the preferred logging needs to be using the functions specified
Whatever debug tracing you are adding for investigation, it needs to remain in code. Do not remove it afterwards.

# Investigation

Before starting a new investigation path, explain me your action plan
If some issue is not understood, before doing guesses, improve logging and tracing to understand the problem better. Follow the investigation paths by following evidence.

# Making changes

Do not use replace tool method of changing files. It is not reliable - Use write_file method to update files.
Do not delete or revert files to last commit ever. If you are not able to solve an issue, stop and let me know.
