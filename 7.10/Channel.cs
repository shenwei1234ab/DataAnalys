using System;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Diagnostics;
using System.IO;

//渠道
public class Channel
{
    //服务器的渠道ID
    public string Id { get; set; }
    public string Name { get; set; }
    //中文在线的渠道ID
    public string ChannelId { get; set; }
}