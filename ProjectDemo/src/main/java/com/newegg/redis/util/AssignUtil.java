package com.newegg.redis.util;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AssignUtil {

	private Set<String> set = new HashSet<String>(); // 机器集合
	private int computerNum;
	private int master; // master 个数
	private int salve; // salve 个数
	private List<Map<String, Object>> collection = new ArrayList<Map<String, Object>>();
	private List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
	private BitSet bitSet = new BitSet();

	public AssignUtil(List<String> list, int master, int salve) {
		if (master + salve != list.size()) {
			// 抛异常
			throw new IllegalArgumentException("master sum salve can't equals list size.");
		}
		this.master = master;
		this.salve = salve;
		for (String str : list) {
			String[] ip = str.split(":");
			String addr = ip[0];
			String port = ip[1];
			set.add(addr);
			int ipNum = Integer.parseInt(addr.replace(".", ""));
			bitSet.set(ipNum, false);
			Map<String, Object> map = new HashMap<String, Object>();
			map.put("addr", addr);
			map.put("ipNum", ipNum);
			map.put("port", port);
			collection.add(map);
		}
		computerNum = this.set.size();
	}

	public void master() {
		int end = 0;
		int count = collection.size() ;
		for (Iterator<Map<String, Object>> iter = collection.iterator(); iter.hasNext();) {
			Map<String, Object> m = iter.next();
			Integer ipNum = (Integer) m.get("ipNum");
			if (!bitSet.get(ipNum) ||  count <= master - result.size() ) {
				bitSet.set(ipNum);
				m.put("role", "master");
				m.put("allotCount", 0);
				m.put("salveNode", new ArrayList<Map<String, Object>>());
				result.add(m);
				iter.remove();
				end++;
				if (end == master) {
					break;
				}
			}
			count-- ;
		}
	}

	public void slave() {
		int max = 1;
		int count = collection.size();
		if (salve >= computerNum) {
			max = (salve / computerNum) + salve % computerNum;
		}
		for (Iterator<Map<String, Object>> iter = collection.iterator(); iter.hasNext();) {
			Map<String, Object> m1 = iter.next();
			String addr1 = (String) m1.get("addr");
			boolean flag = false ;
			for (Iterator<Map<String, Object>> iter2 = result.iterator(); iter2.hasNext();) {
				Map<String, Object> m2 = iter2.next();
				Integer allotCount = (Integer) m2.get("allotCount");
				String addr2 = (String) m2.get("addr");
				if ((allotCount < max  && !addr1.equals(addr2))  || count <= result.size()) {
					m1.put("role", "salve");
					m2.put("allotCount", allotCount + 1);
					List<Map<String, Object>> list = (List<Map<String, Object>>) m2.get("salveNode");
					list.add(m1);
					flag = true;
					count -- ;
					break;
				}
				
			}
			if(flag) continue;
		}
	}

	public List<Map<String, Object>> assign() {
		master();
		slave();
		return result;
	}

	public static void main(String[] args) {
		List<String> list = new ArrayList<String>();

		list.add("10.16.238.71:8080");
		list.add("10.16.238.72:8080");  list.add("10.16.238.71:8080"); list.add("10.16.238.71:8080");
		
		
		 
		
		list.add("10.16.238.71:8080");
		list.add("10.16.238.71:8080");

		for (Map m : new AssignUtil(list,2,4).assign()) {
			System.out.println("----------Master--------");
			System.out.println(m.get("addr"));
			System.out.println("                        salve--------");
			List<Map<String, Object>> list1 = (List) m.get("salveNode");
			for (Map m1 : list1) {
				System.out.println("                        "+m1.get("addr"));
			}
		}
	}

}
