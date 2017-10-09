package eu.stratosphere.nephele.visualization.swt;

import java.util.ArrayList;

import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Text;

import eu.stratosphere.nephele.api.FaultToleranceAPI;
import eu.stratosphere.nephele.event.job.AbstractEvent;

//Brauche PseudoConsole in einer extra Klasse, da die Konsole später parallel 
//nach neuen Events fragen soll ohne sonstigen Programmablauf zu stören.
//Brauche hier also Thread/Runnable Klasse
public class SWTPseudoConsole {
	
	/**
	 * GUI TextField which should print the Events
	 */
	private Text textField = null;
	
	private Display display;
	
	/**
	 * The current FaultToleranceAPI
	 */
	private FaultToleranceAPI api;
	
	/**
	 * Iterator for the EventList
	 */
	private int listCounter;
	
	/**
	 * This class should not run more than one Thread at the same time
	 */
	private boolean threadIsRunning = true;
	
	/**
	 * Default Constructor
	 */
	@SuppressWarnings("unused")
	private SWTPseudoConsole() {
		
	}
	
	public SWTPseudoConsole(Text textField, Display display, FaultToleranceAPI api) {
		this.textField = textField;
		this.display = display;
		this.api = api;
		this.listCounter = 0;
	}
	
	public void addThread() {
		Display.getDefault().asyncExec(new Runnable() {
			@Override
			public void run() {
				while (threadIsRunning) {
					ArrayList<AbstractEvent> eventList = api.getJobClientEventList();
					//if there are not printed Events, print them
					if (eventList != null && eventList.size() > listCounter) {

						for (int i = listCounter; i < eventList.size(); i++) {
							String printableEvent = eventList.get(i).toString();
							textField.append(printableEvent + " \n");
							listCounter++;
						}

					}
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						System.out.println("Cant sleep");
					}
				}
			}
		});
	}
	
	
	public void askForEventsParallel() {
		Thread ask = new Thread(new askForEvents());
		ask.start();
	}
	
	public void stopAskForEvents() {
		this.threadIsRunning = false;
	}
	
	
	public class askForEvents implements Runnable {	
		@Override
		public void run() {
			while (threadIsRunning) {
				ArrayList<AbstractEvent> eventList = api.getJobClientEventList();
				//if there are not printed Events, print them
				if (eventList != null && eventList.size() > listCounter) {

					for (int i = listCounter; i < eventList.size(); i++) {
						String printableEvent = eventList.get(i).toString();
						textField.append(printableEvent + " \n");
						listCounter++;
					}

				}
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
				}
			}
		}
	}

}
