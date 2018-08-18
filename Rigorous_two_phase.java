package TwoPhaseLocking;

/* Authors 
 *  Name: Laukik Kathoke ID: 1001558835
 *  Name: Ambika Pati ID: 1001510620
 */



import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Scanner;



//The core method that implements Two phase lockng protocol
public class Rigorous_two_phase {
	
	public static HashMap<String, Transaction> tran_Table = new HashMap<String, Transaction>();    //Hashmap is defined and declared for Transaction table
	public static HashMap<String, Lock> lock_Table = new HashMap<String, Lock>();                  //Hashmap is defined and declared for Lock table
	public static Integer TS = 0;                                                                  //Globally initializing Timestamp
	
	
 //checkOper method simulates two phase locking protocol and updates Transaction and lock table accordingly.
	public static void checkOper(String input) {
		System.out.println();
		System.out.println(input.substring(0, input.length() - 1));     //gets the current line of operation from input file
		String charZero = input.substring(0, 1);                        //gets the first character from the input operation
		//based on the first character read, a switch case is applied that performs actions relevant to the character read.
		switch (charZero) {
		
			// Begins the transaction  and creates a new row in Transaction table with state Transactionid , timestamp and state as "Active" based on input .
			case "b": {
				
				Transaction tr = new Transaction();
				tr.tran_ID = input.substring(1, input.length() - 1);       
				TS = TS + 1;
				tr.tran_Timestamp = TS;
				tr.tran_State = "Active";
				tran_Table.put(tr.tran_ID, tr);
				System.out.println("Begin Transaction " + tr.tran_ID);
				System.out.println("TranasctionID - "+tr.tran_ID+", Timestamp - " + tr.tran_Timestamp + ", State - "+"Active");
				break;
			}
			//Reads the transaction and provides readlock for item if the state is active and the current lock state is Readlocked.
			//If the lock state is WriteLocked then Write-read conflict arises and Wait_or_Die method is called to decide if the Transaction
			// waits or it aborts
			//Updating the Lock table.
			//Updating the Transaction table.
			case "r": {
				try {
					String item_Value = input.substring(input.indexOf('(') + 1, input.indexOf(')'));     //getting the item value from input
					String tran_id = input.substring(1, input.indexOf('('));                          //getting the transaction ID from input
 
					Transaction t1  = tran_Table.get(tran_id);                                    //getting the current transaction ID from transaction table
					String tran_state = tran_Table.get(tran_id).tran_State;                      //getting the transaction state of the current transaction ID
					//perform only for the Transaction with active state .  
					if (tran_state.equals("Active")) {                                         //Checks if the transaction state is active

						// Appends current Transaction ID in
						// item.tran_holding_lock
						try {
							Lock l1 = lock_Table.get(item_Value);                 //Creating a lock table object and getting current item
							String lock_state;
							lock_state = lock_Table.get(item_Value).lock_state;       //gets the lock state of current item 
							//Checks if the transaction is present in waiting lisr and removes Transaction from waiting list
							if(l1.tran_waiting.contains(tran_id))
							{
								l1.tran_waiting.remove(tran_id);                     //Removing transaction from waiting transactions
							}
							
							//Checking the lock state of the current item and if it is ReadLocked then it grants read lock to the current transaction
							if (lock_state == "RL"  ) {
								try { 
									List tran_Locking;                                     //Declaring a list for Locking transaction
									tran_Locking = lock_Table.get(item_Value).tran_holding_lock;      //Getting all the locking transactions of the current item
									tran_Locking.add(tran_id);                                        //Adding the current transaction to Locking transaction list
									l1.tran_holding_lock = tran_Locking;
									lock_Table.put(item_Value, l1);
									//Displaying the appropriate message
									System.out.println("Operation Successful");
									System.out.println("Read Lock obtained for " + item_Value);
									System.out.println("Updated lock table for " + item_Value + ": Locking Transactions - "+ lock_Table.get(item_Value).tran_holding_lock + " Lock state - " + lock_Table.get(item_Value).lock_state);

									//Updating locked items in transaction table
									List locked_Items;                                           
									locked_Items = tran_Table.get(tran_id).Locked_items;     //getting locked items of ccurrent transactions
									locked_Items.add(item_Value);                            //Adding the current item to locked items of transaction
									t1.Locked_items = locked_Items;
									tran_Table.put(tran_Table.get(tran_id).tran_ID, t1);     //Updating transaction table
									System.out.println("Updated Transaction table for transaction "+tran_id +": Locked items by Transaction " + tran_id + " are - "+ tran_Table.get(tran_id).Locked_items);
								} catch (Exception e1) {  
									//Updating the list of transaction holding lock
									List lock_trans = new ArrayList();
									lock_trans.add(tran_id);                            
									l1.tran_holding_lock = lock_trans;        //Adding current transaction to the list of transaction holding lock
									lock_Table.put(item_Value, l1);
									System.out.println("Operation Successful");
									System.out.println("Read Lock obtained for " + item_Value);
									System.out.println("Updated lock table for " + item_Value + ": Locking Transation - "+ lock_Table.get(item_Value).tran_holding_lock + " Lock state -" + lock_Table.get(item_Value).lock_state);
                                    
									//Updating locked items in the transactiont table
									List add_items = new ArrayList();
									add_items.add(item_Value);
									t1.Locked_items = add_items;
									tran_Table.put(tran_Table.get(tran_id).tran_ID, t1);
									System.out.println("Updated Transaction table for transaction " +tran_id + ": Locked items by Transaction - "+ tran_Table.get(tran_id).Locked_items);
								}
								
							}
							// Checking if the lock state of item is WriteLocked. If it is then there is a Write-Read conflict between the transactions and 
							// Wait_or_Die method is called to resolve the conflict
							else if ( lock_state == "WL"){
							    //System.out.println("Operation not successful");
								System.out.println("Write Read Conflict! between Transaction " + l1.tran_holding_lock.get(0) + " and Transaction " + t1.tran_ID);
								Wait_or_Die(input, item_Value, tran_id);              //Calling Wait_or_Die method due to conflict
							}
							else
							{
								// Append current Transaction ID in
								// tran_holding_lock for the item in lock table
								Lock l2 = lock_Table.get(item_Value);
								l2.lock_state = "RL";
								List tran_Put = new ArrayList();
								tran_Put.add(tran_id);
								l2.tran_holding_lock = tran_Put;
								lock_Table.put(item_Value, l2);
								System.out.println("Operation Successful");
								System.out.println("Read Lock acquired for " + item_Value);
								System.out.println("Updated lock table for " + item_Value + ": Locking Transaction - "+ lock_Table.get(item_Value).tran_holding_lock + " Lock state - "+lock_Table.get(item_Value).lock_state) ;

								// Update Locked_items in Transaction_Table where
								// Transaction_Id = tran_id
								try {
									List locked_Items;
									locked_Items = tran_Table.get(tran_id).Locked_items;
									locked_Items.add(item_Value);
									t1.Locked_items = locked_Items;
									tran_Table.put(tran_Table.get(tran_id).tran_ID, t1);
									System.out.println("Updated Tansaction table for transaction " + tran_id + ": Locked items by Transaction - " + tran_Table.get(tran_id).Locked_items);

								} catch (Exception e3) {
									List tran_Put1 = new ArrayList();
									tran_Put1.add(item_Value);
									t1.Locked_items = tran_Put1;
									tran_Table.put(tran_Table.get(tran_id).tran_ID, t1);
									System.out.println("Updated Tansaction table for transaction " + tran_id + ": Locked items by Transaction - " + tran_Table.get(tran_id).Locked_items);

								}
							}
						}
						// Append current Transaction ID in
						// item.tran_holding_lock.
						catch (Exception e2) {
							Lock l3 = new Lock();
							l3.item_name = item_Value;
							l3.lock_state = "RL";
							List tran_Put2 = new ArrayList();
							tran_Put2.add(tran_id);
							l3.tran_holding_lock = tran_Put2;
							lock_Table.put(item_Value, l3);
							System.out.println("Operation Successful");
							System.out.println("Read Lock obtained for " + item_Value);
							System.out.println("Updated Lock table for " + item_Value + ": Locking Transactions - "+ lock_Table.get(item_Value).tran_holding_lock+ " Lock State - " + lock_Table.get(item_Value).lock_state);
							// Update Locked_items in Transaction_Table where
							// Transaction_Id = tran_id
							try {
								List locked_Items;
								locked_Items = tran_Table.get(tran_id).Locked_items;
								locked_Items.add(item_Value);
								t1.Locked_items = locked_Items;
								tran_Table.put(tran_Table.get(tran_id).tran_ID, t1);
								System.out.println("Updated Tansaction table for transaction " + tran_id + ": Locked items by Transaction - " + tran_Table.get(tran_id).Locked_items);

							} catch (Exception e3) {
								List add_items = new ArrayList();
								add_items.add(item_Value);
								t1.Locked_items = add_items;
								tran_Table.put(tran_Table.get(tran_id).tran_ID, t1);
								System.out.println("Updated Tansaction table for transaction " + tran_id + ": Locked items by Transaction - " + tran_Table.get(tran_id).Locked_items);
							}
						}

					}
					// Checks if transaction state is blocked and  then the current operation is added to waiting operations in Transaction table.
					else if (tran_state == "blocked") {
						try {
							List tran_waiting;
							tran_waiting = tran_Table.get(tran_id).oper_Waiting;
							tran_waiting.add(input);                                //Adding the current blocked operation in oper waiting list in transaction table
							t1.oper_Waiting = tran_waiting;
							tran_Table.put(tran_id, t1);
							System.out.println("Current transaction is blocked and operation " + input
									+ " is added to waiting Operations");
							System.out.println("Waiting Operations of Transaction "+tran_id+" are "+ tran_waiting);
						} catch (Exception e) {
							t1.oper_Waiting.add(input);
							tran_Table.put(tran_id, t1);
							System.out.println("Current transaction is blocked and operation " + input
									+ " is added to waiting Operations");
							System.out.println("Waiting operations of Transaction"+tran_id+" are"+t1.oper_Waiting);
						}
					}
					// If the transaction is aborted then the appropriate message is displayed.
					else {
						System.out.println("Operation unsuccessful");
						System.out.println("Transaction " + tran_id + " is already aborted");
					}
				} catch (Exception e4) {
					
					System.out.println("Transaction is not active");
				}
				break;
			}
			//Write operation is performed on reading w if the transaction state is active and unlocked. The item obtains write lock on a particular item.
			//If the items lock state is write locked or read locked then conflict arises and Wait_or_die method is called to resolve it
		
			case "w": {
				String item_Value = input.substring(input.indexOf('(') + 1, input.indexOf(')'));          //getting the item from the input
				String tran_id = input.substring(1, input.indexOf('('));                                  //getting transaction id from the input
				Transaction tr = (Transaction) tran_Table.get(tran_id);
				Lock item;
				//Checking if the item is present in lock tableand if not present then creating it
				if (lock_Table.containsKey(item_Value)) {
					item = lock_Table.get(item_Value);
				} else {
					item = new Lock();                 
					item.lock_state = "UL";
					item.item_name = item_Value; 
					lock_Table.put(item_Value, item);             //creating the specified item with lock state Unlocked in the lock table
				}
				//Checking the state of transaction and perorms only if it is active 
				if (tr.tran_State == "Active") {
					//If the current lock state of the item is read lock then check if the same transaction as current transaction holds that lock.
					//If the same transaction holds the lock then upgrade the read lock to write lock and if the transactions are
					//different then call Wait_or_Die method to resolve the conflict
					
					if (item.lock_state.equals("RL")) {
						// Checking if the same transaction holds the lock and upgrading the lock to Write lock
						if (item.tran_holding_lock.size() == 1 && item.tran_holding_lock.contains(tran_id)) {
							item.lock_state = "WL";
							System.out.println("Operation successful");
							System.out.println("Transaction " + tran_id + " upgraded item " + item_Value + " to WL ");
							System.out.println("Updated Lock table for " + item_Value + ": Locking Transaction - "+ lock_Table.get(item_Value).tran_holding_lock + " Lock state - "+lock_Table.get(item_Value).lock_state );
							System.out.println("Updated Transaction table for Transaction " + tran_id + ": Locked items by transaction - " + tran_Table.get(tran_id).Locked_items);
							try{
								item.tran_waiting.remove(tran_id);                  //Removing current transaction from waiting transactions, as it started execution
							}
							catch (Exception e2){
							}
						}
						//Checking the transactions holding readlocks for the item and if other transactions hold read lock for the item then 
						//read write conflict arises
						
						else if (item.tran_holding_lock.size() > 1 && item.tran_holding_lock.contains(tran_id)) {
							//System.out.println("Operation unsuccessful");
							System.out.println("Read Write Conflict! between Transaction " + item.tran_holding_lock + " and Transaction " + tr.tran_ID);
							Wait_or_Die(input, item_Value, tran_id);             //Calling Wait_or_Die method
						}
						// If some other transaction holds read lock
						else if (!item.tran_holding_lock.contains(tran_id)) {
							if (item.tran_holding_lock.isEmpty()) {
								item.lock_state = "WL";
								item.tran_holding_lock.add(tran_id);
								System.out.println("Operation successful");
								System.out.println("Transaction " + tran_id + " upgraded item " + item_Value + " to WL ");
								System.out.println("Updated Lock table for " + item_Value + ": Locking Transaction - "+ lock_Table.get(item_Value).tran_holding_lock+" Lock State - " + lock_Table.get(item_Value).lock_state);
								System.out.println("Updated Transaction table for Transaction " + tran_id + ": Locked items by transaction - " + tran_Table.get(tran_id).Locked_items);
								//Removing current transaction from waiting transactions, as it started execution
								try{
									item.tran_waiting.remove(tran_id);
								}
								catch (Exception e2){
								}
							} else {
								Wait_or_Die(input, item_Value, tran_id);
							}
						}
					} //If the lock state is Write Lock then write-write conflict arises. It calls the wait die method to resolve the conflict.
					else if (item.lock_state == "WL") {
						// Applying Die-Wait logic here
						//System.out.println("Operation unsuccessful");
						System.out.println("Write Write Conflict between Transaction " + item.tran_holding_lock.get(0) + " and Transaction " + tr.tran_ID);
						Wait_or_Die(input, item_Value, tran_id);
					} else if (item.lock_state == "UL") {
						System.out.println("Operation successful");
						System.out.println("Transaction " + tran_id + " acquired WL on item "+ item_Value);
						//System.out.println("Updated Lock table for " + item_Value + ": Locking Transactios - "+ lock_Table.get(item_Value).tran_holding_lock);
						List<String> tmpList = item.tran_holding_lock;
						tmpList.add(tran_id);
						item.tran_holding_lock = tmpList;
						item.lock_state = "WL";
						tr.Locked_items.add(item_Value);
						System.out.println("Updated Lock table for " + item_Value + ": Locking Transaction -  "+ lock_Table.get(item_Value).tran_holding_lock + " Lock state - "+lock_Table.get(item_Value).lock_state);
						System.out.println("Updated Transaction table for Transaction " + tran_id + ": Locked items by transaction - " + tran_Table.get(tran_id).Locked_items);
					}
					//Check the transaction state if blocked then the operation is a added to waiting operations
				
				} else if (tr.tran_State.equals("blocked")) {
					tr.oper_Waiting.add(input);
					System.out.println("Current transaction is blocked and operation " + input
							+ " is added to waiting Operations");
					System.out.println("Waiting operations of Transaction"+tran_id+" are"+tr.oper_Waiting);
				}
				// Ignoring the current transaction if the transaction state is aborted
				else {
					System.out.println("Operation unsuccessful");
					System.out.println("Transaction " + tran_id + " is already aborted");
				}
				break;
			}
			//Ending the transaction. It commits the transaction and aborts the transaction by releasing all locks held.
			//It calls Execute_wait_transaction to run waiting transactions if any

			case "e": {
				String trans_id = input.substring(1, input.length() - 1);
				Transaction tr = (Transaction) tran_Table.get(trans_id);
				//If the transaction state is active then it is changed to committed
				if (tr.tran_State == "Active") {
					tr.tran_State = "Committed";
					System.out.println("End transaction " + trans_id);
					System.out.println("Transaction " + trans_id + " is committed ");
				}
				// if transaction state is blocked then the current operation is added to waiting operations in Transaction table of that transaction.
				else if (tr.tran_State.equals("Blocked")) {
					tr.oper_Waiting.add(input);
					System.out.println("Current transaction is blocked and operation " + input
							+ " is added to waiting Operations");
					System.out.println("Waiting operations of Transaction"+trans_id+" are"+tr.oper_Waiting);
					break;
				}
				// Ignoring the current tansaction if the state is aborted
				else {
					System.out.println("Operation unsuccessful");
					System.out.println("Transaction " + trans_id + " is already aborted");
					break;
				}
				tr.Locked_items.clear();                                    //Releasing the locks held by the transaction
				// Releasing locks by trans_id in Lock Table
				for (String key : lock_Table.keySet()) {
					Lock item = lock_Table.get(key);
					// Removing current transaction from Locking Transactions column of
					// item.
					if (item.tran_holding_lock.contains(trans_id)) {
						item.tran_holding_lock.remove(trans_id);
							System.out.println("Lock held by Transaction "+trans_id+ " for item "+item.item_name + " is released");
						if (item.tran_holding_lock.isEmpty()) {                                //Checking if the items held by transaction is empty
							if (item.tran_waiting.isEmpty()) {
								//Checks if there are any transactions waiting on the item
								//if no transactions waiting then the item state is changed to unlock
								item.lock_state = "UL";
								System.out.println("As there are no waiting transaction item " + item.item_name + " is unlocked");
							} else {
								Execute_tran_waiting(item.tran_waiting);           //Running waiting transactions
							}
						}
						else{
							Execute_tran_waiting(item.tran_waiting);                //unning waiting transactions
						}
					}
					// Remove aborted transaction from Waiting Transactions List
					if (item.tran_waiting.contains(trans_id)) {
						item.tran_waiting.remove(trans_id);
					}
				}
				break;
			}
			default:
				System.out.println("Invalid input");
				break;
		}
	}
// Wait_or_Die method checks if requesting Transaction is older or not. if the timestamp of conflicting transaction  is older to that of locking transaction it waits otherwise it aborts.
	private static void Wait_or_Die(String input, String item_Value, String tran_id) {
		Transaction t_young = tran_Table.get(tran_id);                     //gets the transaction id of current transaction
		Lock lck1 = lock_Table.get(item_Value);                           
		List write_locked_trans = new ArrayList();
		write_locked_trans = lck1.tran_holding_lock;
		//Looping through all the write locked transaction to find all the write locked transactions of the item 
		for (int i=0 ;i<write_locked_trans.size();i++)
		{
			Transaction t_old = tran_Table.get(write_locked_trans.get(i));         //t_old contains transaction ids of transaction holding Write lock on item
			if (t_old!=t_young)
			{
				int tj = t_old.tran_Timestamp;                         //tj contains timestamp of locked transactions
				int ti = t_young.tran_Timestamp;                       //ti contains timestamp of conflicting transacion
				//Wait Die condition to check if the transaction aborts or waits
				if (ti < tj) {
					//Transaction waits as ti is older than tj
					System.out.println("Operation Successful");
					System.out.println("Transaction  "+t_young.tran_ID + " Waits as Timestamp of "+ t_young.tran_ID + " is less than Transaction" + t_old.tran_ID);
					//Wait_Transaction(tr.tran_ID, input);
					t_young.tran_State = "Blocked";                   //Changing the transaction state to blocked
					System.out.println("Current transaction is blocked and operation " + input
							+ " is added to waiting Operations");
					t_young.oper_Waiting.add(input);                       //Adding the operation to waiting operations list
					System.out.println("Waiting operations of Transaction"+t_young.tran_ID+" are"+t_young.oper_Waiting);
					String item_Val = input.substring(input.indexOf('(') + 1, input.indexOf(')'));
					Lock lck= lock_Table.get(item_Val);
					lck.tran_waiting.add(tran_id);

				} else {
					
					//Transaction aborts as ti is younger than tj
					System.out.println("Operation unsuccessful");
					System.out.println("Transaction "+t_young.tran_ID + " Aborts as Timestamp of "+ t_young.tran_ID + " is greater than Transaction" + t_old.tran_ID);
					
					t_young.tran_State = "Aborted";         //Changing transactions state to aborted
					try {
						t_young.Locked_items.clear();       //Releasing the locks
					}
					catch (Exception e)
					{
//						System.out.println("No items held by this transaction");
					}
					// Releasing locks by trans_id in Lock Table
					for (String key : lock_Table.keySet()) {
						Lock item = lock_Table.get(key);
						// Removing current transaction from Locking Transactions column of
						// item.
						if (item.tran_holding_lock.contains(t_young.tran_ID) ) {
							item.tran_holding_lock.remove(t_young.tran_ID);
							System.out.println("Lock held by Transaction " + t_young.tran_ID + " for item " +item.item_name+" is released");
							if (item.tran_holding_lock.isEmpty()) {
								if (item.tran_waiting.isEmpty()) {
									// The items lock state is changed to unlocked as there are no transaction waiting on it
									
									item.lock_state = "UL";
									System.out.println(" The item "+ item.item_name + " is unlocked as there are no waiting transactions");
								} else {
									Execute_tran_waiting(item.tran_waiting);    //Running waiting transactions
								}
							}
							else{
								Execute_tran_waiting(item.tran_waiting);
							}
						}

						// Removing aborted transaction from Waiting Transactions List
						if (item.tran_waiting.contains(t_young.tran_ID)) {

							item.tran_waiting.remove(t_young.tran_ID);
						}
					}
					
				}
			}
		}
	}
	
	

//Execute_tran_waiting methods simply pass the stored operation of the blocked transaction to checkOper method.
	private static void Execute_tran_waiting(List<String> tran_waiting) {
	// Executing all waiting operations of First waiting Transaction.
		for (int k = 0; k < tran_waiting.size(); k++) {
			
			Transaction t4 = tran_Table.get(tran_waiting.get(k));
			System.out.println("Started waiting transaction " + tran_waiting.get(k));
			System.out.println("Transaction State - "+t4.tran_State+" .Waiting operation of the transaction - "+t4.oper_Waiting );
			for (ListIterator<String> iter = t4.oper_Waiting.listIterator(); iter.hasNext();) {
				String inpStr = iter.next();
				t4.tran_State = "Active";
				checkOper(inpStr);
				
			}
		}
	}
	
	public static void main(String[] args) {
     //Reads input file using a scanner. It parses the line using checkOper() method
		try{
		Scanner user = new Scanner(System.in);
		 String inputFile;
		 
		 System.out.print("Input file name: ");
		 inputFile = user.nextLine().trim();
		 File input = new File(inputFile);
		 Scanner scan = new Scanner(input);
		 
	     String statement;
		 
		 while((statement = scan.nextLine())!=null){
			 
			 statement = statement.replace(" ", "");
			 statement = statement.trim();
			 
			 checkOper(statement);
			 
			 }
		}
		catch(Exception e){
			System.out.println();
		System.out.println("End of file");
		}
	} 

}







/*References:
1) http://javatutorialhq.com/java/util/hashmap-class/
2) “Ways to Iterate over a List in Java.” Stack Overflow, stackoverflow.com/questions/18410035/ways-to-iterate-over-a-list-in-java.
3) “Two-Phase Locking.” Wikipedia, Wikimedia Foundation, 12 July 2018, en.wikipedia.org/wiki/Two-phase_locking.
5)  “Different Ways of Reading a Text File in Java.” GeeksforGeeks, 24 Mar. 2018, www.geeksforgeeks.org/different-ways-reading-text-file-java/.
6)  "Fundamentals of database systems" Ramez Elmasri and Shashikant Navathe, 6th Edition.
7)  “DBMS | Concurrency Control Protocol | Two Phase Locking (2-PL)-I.” GeeksforGeeks, 20 Mar. 2018, www.geeksforgeeks.org/dbms-concurrency-control-protocols-two-phase-locking-2-pl/.
8)  Tutorials Point. “DBMS Deadlock.” Www.tutorialspoint.com, Tutorials Point, 21 July 2018, www.tutorialspoint.com/dbms/dbms_deadlock.htm.
*/