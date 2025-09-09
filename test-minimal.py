#!/usr/bin/env python3

import urwid

def test_basic_layout():
    # Create a simple layout without any Columns
    text1 = urwid.Text("Test text 1")
    text2 = urwid.Text("Test text 2")
    
    pile = urwid.Pile([
        ('pack', text1),
        ('pack', text2)
    ])
    
    fill = urwid.Filler(pile, valign='top')
    
    def exit_on_q(key):
        if key in ('q', 'Q'):
            raise urwid.ExitMainLoop()
    
    loop = urwid.MainLoop(fill, unhandled_input=exit_on_q)
    loop.run()

if __name__ == "__main__":
    test_basic_layout()
