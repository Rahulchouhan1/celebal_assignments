-- Query 6: Display students' names, grades, and marks.
-- If grade is 8 or more, show name and sort by name.
-- If grade is less than 8, hide name and sort by marks.
SELECT
  CASE WHEN g.Grade >= 8 THEN s.Name ELSE NULL END AS Name,
  g.Grade,
  s.Marks
FROM Students s
JOIN Grades g
  ON s.Marks BETWEEN g.Min_Mark AND g.Max_Mark
ORDER BY
  g.Grade DESC,
  CASE WHEN g.Grade >= 8 THEN s.Name END ASC,
  CASE WHEN g.Grade < 8 THEN s.Marks END ASC;
