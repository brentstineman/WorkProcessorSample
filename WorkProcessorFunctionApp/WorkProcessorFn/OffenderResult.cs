using System;
using System.Collections.Generic;
using System.Text;

namespace WorkProcessorFn
{
    class OffenderResult
    {
        public string Forensic_ID { get; set; }
        public string Offender_ID { get; set; }
        public string Amelogenin_For { get; set; }
        public string Amelogenin_Off { get; set; }
        public int Loci_Shared { get; set; }
        public int Loci_Not_Shared { get; set; }
        public int Alleles_Shared { get; set; }
        public float PI_freq1 { get; set; }
        public float PI_freq2 { get; set; }
        public float PI_freq3 { get; set; }
        public float PI_Local { get; set; }
        public float SIB_freq1 { get; set; }
        public float SIB_freq2 { get; set; }
        public float SIB_freq3 { get; set; }
        public float SIB_Local { get; set; }
        public float HalfI_freq1 { get; set; }
        public float HalfI_freq2 { get; set; }
        public float HalfI_freq3 { get; set; }
        public float HalfI_Local { get; set; }
        public float maxLR { get; set; }
        public float maxPI { get; set; }
        public float Maxsib { get; set; }
    }
}
