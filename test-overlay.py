#!/usr/bin/env python3

import urwid

def create_safe_overlay(dialog, main_widget, width=50, height=10):
    """Create an overlay with safe sizing"""
    try:
        # Ensure minimum constraints
        if isinstance(width, int) and width > 80:
            width = 80
        if isinstance(height, int) and height > 25:
            height = 25
            
        overlay = urwid.Overlay(
            dialog, main_widget,
            align='center', width=width,
            valign='middle', height=height
        )
        return overlay
    except Exception as e:
        print(f"Error creating overlay: {e}")
        return main_widget

def test_overlay():
    # Test creating overlays with various sizes
    main_text = urwid.Text("Main content")
    main_fill = urwid.Filler(main_text)
    
    dialog_text = urwid.Text("Dialog content")
    dialog = urwid.LineBox(dialog_text, title="Test Dialog")
    
    overlay = create_safe_overlay(dialog, main_fill, 40, 8)
    
    def exit_on_q(key):
        if key in ('q', 'Q'):
            raise urwid.ExitMainLoop()
    
    loop = urwid.MainLoop(overlay, unhandled_input=exit_on_q)
    loop.run()

if __name__ == "__main__":
    test_overlay()
