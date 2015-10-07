package p3.ip.analyzer;

import java.math.BigInteger;

public class PacketInfo implements Comparable<PacketInfo>
{
	Long ps;	
	Long arrival_s;
	Long arrival_us;
	Long ipt;
	
	PacketInfo (Long arrival_s, Long arrival_us, Long ps)
	{
		this.arrival_s = arrival_s;		
		this.arrival_us = arrival_us;
		this.ps = ps;
	}

	public int compareTo(PacketInfo o) {
		// TODO Auto-generated method stub

		long diff_s = this.arrival_s - o.arrival_s;
		long diff_us = this.arrival_us - o.arrival_us;
		
		if (diff_s == 0) {
			return (int) diff_us; 
		}
		else if (diff_s > 0) {
			return 2000000;
		}
		else return -2000000;
		
	}

	long ten_to_the_ten = 10000000000L;
	long ten_to_the_sixth = 1000000L;
	
	public long ipt(PacketInfo previous) {
		// TODO Auto-generated method stub

		long diff_s = this.arrival_s - previous.arrival_s;
		long diff_us = this.arrival_us - previous.arrival_us;
		
		if (diff_s == 0) {
			return diff_us; 
		}
		else 
		{
			if (diff_s > - ten_to_the_ten && diff_s < ten_to_the_ten)
			{
				return diff_s * ten_to_the_sixth + diff_us;
			}
			else return -123456789; // should change this to throw an exception 
		}
	}

	
}
