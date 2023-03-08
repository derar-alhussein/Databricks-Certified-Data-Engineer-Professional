# Databricks notebook source
class Cube:
    def __init__(self, edge):
        self.edge = edge
    
    def get_volume(self):
        a = self.edge
        return a * a * a
        
    
    def get_surface_area(self):
        a = self.edge
        return 6 * a * a
