using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace mongodb_labs
{
    public class Logger
    {
        public void Error(string Message)
        {
            Log("ERROR", Message);
        }

        public void Warning(string Message)
        {
            Log("WARN", Message);
        }

        public void Info(string Message)
        {
            Log("INFO", Message);
        }

        public void Debug(string Message)
        {
            Log("DEBUG", Message);
        }

        private void Log(string Severity, string Message)
        {
            StringBuilder message = new StringBuilder();

            // Create the message
            message.Append(System.DateTime.Now.ToString()).Append(", ").Append(Severity.ToUpper()).Append(", ").Append(Message);

            Console.WriteLine(message.ToString());
        }
    }
}
