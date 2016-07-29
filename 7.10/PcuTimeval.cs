using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Diagnostics;
using System.IO;


public class PcuTimeval:IComparable
{
    public PcuTimeval(DateTime datetime, UInt32 num)
    {
        m_pcuTime = datetime;
        m_num = num;
    }

    //pcu时间
    public DateTime m_pcuTime;
    //人数
    public UInt32 m_num;


    public int CompareTo(object obj)
    {
        int result;
        try
        {
            PcuTimeval info = obj as PcuTimeval;
            if (this.m_num > info.m_num)
            {
                result = 1;
            }
            else if (this.m_num < info.m_num)
            {
                result = -1;
            }
            else{
                result = 0;
            }
                
            return result;
        }
        catch (Exception ex) { throw new Exception(ex.Message); }
    }
}