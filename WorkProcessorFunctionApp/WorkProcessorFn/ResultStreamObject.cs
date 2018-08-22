using System;
using System.Collections.Generic;
using System.Text;

namespace WorkProcessorFn
{
    class ResultStreamObject
    {
        public string SchemaVersion { get; set; }
        public string JobDetails { get; set; }
        public SuspectObject SuspectDetails { get; set; }
        public List<OffenderObject> OffenderList { get; set; }
    }
}
