package belsSWTplayground;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.PaintEvent;
import org.eclipse.swt.events.PaintListener;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.events.ShellListener;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;

public class SWTShellEvents {

	/*public static void main(String[] args) {
	    final Display display = new Display();
	    final Shell shell = new Shell(display, SWT.SHELL_TRIM);
	    shell.setLayout(new FillLayout());

	    shell.addShellListener(new ShellListener() {
	      public void shellActivated(ShellEvent event) {
	        System.out.println("activate");
	      }
	      public void shellClosed(ShellEvent arg0) {
	        System.out.println("close");
	      }
	      public void shellDeactivated(ShellEvent arg0) {
	    	  System.out.println("deactivated");
	      }
	      public void shellDeiconified(ShellEvent arg0) {
	    	  System.out.println("deiconified");
	      }
	      public void shellIconified(ShellEvent arg0) {
	    	  System.out.println("iconified");
	      }
	    });

	    shell.open();
	    // Set up the event loop.
	    while (!shell.isDisposed()) {
	      if (!display.readAndDispatch()) {
	        // If no more entries in event queue
	        display.sleep();
	      }
	    }
	    display.dispose();
	  }*/
	
	
	//Change the default behavior of shells
	/*public static void main(String[] args) {
	    final Display display = new Display();
	    final Shell shell = new Shell(display, SWT.SHELL_TRIM);
	    shell.setLayout(new FillLayout());
	    //Alle unteren Funktionen müssen vorhanden sein
	    shell.addShellListener(new ShellListener() {
	      public void shellActivated(ShellEvent event) {}
	      public void shellClosed(ShellEvent event) {
	        MessageBox messageBox = new MessageBox(shell, SWT.APPLICATION_MODAL | SWT.YES | SWT.NO);
	        messageBox.setText("Warning");
	        messageBox.setMessage("You have unsaved data. Close the shell anyway?");
	        if (messageBox.open() == SWT.YES)
	          event.doit = true;
	        else
	          event.doit = false;
	      }
	      public void shellDeactivated(ShellEvent arg0) {}
	      public void shellDeiconified(ShellEvent arg0) {}
	      public void shellIconified(ShellEvent arg0) {}
	    });

	    shell.open();
	    // Set up the event loop.
	    while (!shell.isDisposed()) {
	      if (!display.readAndDispatch()) {
	        // If no more entries in event queue
	        display.sleep();
	      }
	    }
	    display.dispose();
	  }*/
	
	//Add Close event for Shell 
	/*public static void main(String[] args) {
	    Display display = new Display();
	    final Shell shell = new Shell(display);
	    shell.addListener(SWT.Close, new Listener() {
	      public void handleEvent(Event event) {
	        System.out.println("cose");
	      }
	    });
	    shell.pack();
	    shell.open();
	    while (!shell.isDisposed()) {
	      if (!display.readAndDispatch())
	        display.sleep();
	    }
	    display.dispose();
	  }*/
	
	
	//Add Resize listener to Shell
	/*public static void main (String [] args) {
		  Display display = new Display ();
		  final Shell shell = new Shell (display, SWT.SHELL_TRIM | SWT.H_SCROLL | SWT.V_SCROLL);

		  shell.addListener (SWT.Resize,  new Listener () {
		    public void handleEvent (Event e) {
		      Rectangle rect = shell.getClientArea ();
		      System.out.println(rect);
		    }
		  });
		  shell.open ();
		  while (!shell.isDisposed()) {
		    if (!display.readAndDispatch ()) display.sleep ();
		  }
		  display.dispose ();
		}*/
	
	
	public static void main (String [] args) {
		  Display display = new Display ();
		  final Shell shell = new Shell (display, SWT.SHELL_TRIM | SWT.H_SCROLL | SWT.V_SCROLL);

		  final Composite composite = new Composite (shell, SWT.BORDER);
		  composite.setSize (700, 600);

		  final Color red = display.getSystemColor (SWT.COLOR_RED);
		  composite.addPaintListener (new PaintListener() {
		    public void paintControl (PaintEvent e) {
		      e.gc.setBackground (red);
		      e.gc.fillOval (5, 5, 690, 590);
		    }
		  });

		  shell.open ();
		  while (!shell.isDisposed()) {
		    if (!display.readAndDispatch ()) display.sleep ();
		  }
		  display.dispose ();
		}
	
	//
	
	
}
