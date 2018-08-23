using System;
using System.Collections.Generic;
using System.Text;

namespace WorkProcessorFn
{
    public class ResultStreamObject
    {
        public string SchemaVersion { get; set; }
        public string JobDetails { get; set; }
        public List<OffenderResult> ResultList { get; set; }
    }
}
