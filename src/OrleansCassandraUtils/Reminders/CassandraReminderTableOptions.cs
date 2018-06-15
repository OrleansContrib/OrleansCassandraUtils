using Orleans;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OrleansCassandraUtils.Reminders
{
    public class CassandraReminderTableOptions
    {
        [RedactConnectionString]
        public string ConnectionString { get; set; }
    }
}
