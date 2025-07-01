# testing.py
import unittest
import pandas as pd
from plotly_graphs import detect_graph_request, generate_plotly_chart
from app import get_openai_response

class TestPlotlyGraphs(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame({
            "Year": ["2020", "2021", "2022"],
            "New": [100, 120, 150],
            "Churn": [50, 60, 70],
            "measure": ["relative", "relative", "relative"]
        })
        def mock_openai_response(messages):
            if "Does this query request a graph?" in messages[1]["content"]:
                chart_types = {
                    "line graph": "line",
                    "scatter": "scatter",
                    "box plot": "box",
                    "area": "area",
                    "waterfall": "waterfall",
                    "heatmap": "heatmap"
                }
                for key, value in chart_types.items():
                    if key in messages[1]["content"].lower():
                        return f'{{"chart_type": "{value}", "x_col": "Year", "y_col": ["New", "Churn"]}}'
                return '{"chart_type": "line", "x_col": "Year", "y_col": ["New", "Churn"]}'
            return "Mock response"
        self.get_openai_response = mock_openai_response

    def test_detect_graph_request(self):
        result = detect_graph_request("Show a line graph of new and churned clients by year", self.df.columns, self.get_openai_response)
        self.assertEqual(result, {"chart_type": "line", "x_col": "Year", "y_col": ["New", "Churn"]})

    def test_generate_plotly_line(self):
        fig, error = generate_plotly_chart(self.df, "line", "Year", ["New", "Churn"])
        self.assertIsNone(error)
        self.assertIsNotNone(fig)
        self.assertEqual(fig.layout.title.text, "New, Churn by Year")

    def test_generate_plotly_scatter(self):
        fig, error = generate_plotly_chart(self.df, "scatter", "Year", ["New", "Churn"])
        self.assertIsNone(error)
        self.assertIsNotNone(fig)

    def test_generate_plotly_box(self):
        fig, error = generate_plotly_chart(self.df, "box", "Year", ["New", "Churn"])
        self.assertIsNone(error)
        self.assertIsNotNone(fig)

    def test_generate_plotly_area(self):
        fig, error = generate_plotly_chart(self.df, "area", "Year", ["New", "Churn"])
        self.assertIsNone(error)
        self.assertIsNotNone(fig)

    def test_generate_plotly_waterfall(self):
        fig, error = generate_plotly_chart(self.df, "waterfall", "Year", ["New"])
        self.assertIsNone(error)
        self.assertIsNotNone(fig)

    def test_generate_plotly_heatmap(self):
        fig, error = generate_plotly_chart(self.df, "heatmap", "Year", ["New", "Churn"])
        self.assertIsNone(error)
        self.assertIsNotNone(fig)

if __name__ == "__main__":
    unittest.main()